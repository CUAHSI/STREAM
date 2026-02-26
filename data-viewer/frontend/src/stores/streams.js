import { defineStore } from 'pinia'
import { ref } from 'vue'
import { ENDPOINTS } from '@/constants'

export const useStreamsStore = defineStore('streams', () => {
  const sessionToken = ref('')
  const sessionExpiresAt = ref(null)
  const options = ref({
    water_quality_variables: {},
    other_datasets: []
  })
  const gaugesGeoJson = ref(null)
  const selectedGaugeIds = ref([])
  const selectedDomainCodes = ref([])
  const selectedDomainGaugeMap = ref({})
  const zoomToSelectionRequestId = ref(0)
  const selectionBy = ref('gauge') // gauge | domain
  const selectionType = ref('Single') // Single | Multiple
  const loading = ref({
    login: false,
    options: false,
    gauges: false,
    download: false
  })

  async function login(username, password) {
    loading.value.login = true
    try {
      const response = await fetch(ENDPOINTS.streamsLogin, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      })
      const data = await response.json()
      if (!response.ok) {
        throw new Error(data.detail || 'Login failed')
      }
      sessionToken.value = data.session_token
      sessionExpiresAt.value = data.expires_at
      await Promise.all([fetchOptions(), fetchGauges()])
      return data
    } finally {
      loading.value.login = false
    }
  }

  async function logout() {
    if (!sessionToken.value) {
      return
    }
    try {
      await fetch(ENDPOINTS.streamsLogout, {
        method: 'POST',
        headers: {
          'X-Streams-Session': sessionToken.value
        }
      })
    } catch (error) {
      console.warn('Failed to notify logout endpoint', error)
    } finally {
      clearSession()
    }
  }

  function clearSession() {
    sessionToken.value = ''
    sessionExpiresAt.value = null
    gaugesGeoJson.value = null
    clearSelections()
  }

  function isSessionExpired() {
    if (!sessionExpiresAt.value) return false
    const expiresAt = new Date(sessionExpiresAt.value)
    if (Number.isNaN(expiresAt.getTime())) return false
    return expiresAt.getTime() <= Date.now()
  }

  function ensureValidSession() {
    if (!sessionToken.value) {
      throw new Error('Please sign in before continuing.')
    }
    if (isSessionExpired()) {
      clearSession()
      throw new Error('Your STREAMS session has expired. Please sign in again.')
    }
  }

  async function fetchOptions() {
    loading.value.options = true
    try {
      const response = await fetch(ENDPOINTS.streamsOptions)
      const data = await response.json()
      if (!response.ok) {
        throw new Error(data.detail || 'Failed to load STREAMS options')
      }
      options.value = data
    } finally {
      loading.value.options = false
    }
  }

  async function fetchGauges(maxFeatures = 5000) {
    if (!sessionToken.value) return null
    ensureValidSession()
    loading.value.gauges = true
    try {
      const response = await fetch(`${ENDPOINTS.streamsGauges}?max_features=${maxFeatures}`, {
        headers: {
          'X-Streams-Session': sessionToken.value
        }
      })
      const data = await response.json()
      if (!response.ok) {
        if (data?.detail === 'Invalid or expired STREAMS session token.') {
          clearSession()
          throw new Error('Your STREAMS session expired. Please sign in again.')
        }
        throw new Error(data.detail || 'Failed to load gauge data')
      }
      gaugesGeoJson.value = data
      return data
    } finally {
      loading.value.gauges = false
    }
  }

  function setSelectionBy(mode) {
    selectionBy.value = mode
    clearSelections()
  }

  function setSelectionType(mode) {
    selectionType.value = mode
    clearSelections()
  }

  function clearSelectedGauges() {
    selectedGaugeIds.value = []
  }

  function clearSelectedDomains() {
    selectedDomainCodes.value = []
    selectedDomainGaugeMap.value = {}
  }

  function clearSelections() {
    clearSelectedGauges()
    clearSelectedDomains()
  }

  function toggleGaugeSelection(gaugeId) {
    if (!gaugeId) return
    if (selectionType.value === 'Single') {
      selectedGaugeIds.value = [gaugeId]
      return
    }

    if (selectedGaugeIds.value.includes(gaugeId)) {
      selectedGaugeIds.value = selectedGaugeIds.value.filter((id) => id !== gaugeId)
      return
    }
    selectedGaugeIds.value = [...selectedGaugeIds.value, gaugeId]
  }

  function setSelectedGauges(gaugeIds = []) {
    selectedGaugeIds.value = [...new Set(gaugeIds.filter(Boolean))]
  }

  function toggleDomainSelection(domainCode, gaugeIdsForDomain = []) {
    if (!domainCode) return
    const nextMap = { ...selectedDomainGaugeMap.value }
    const nextGaugeIds = [...new Set(gaugeIdsForDomain)].filter(Boolean)

    if (selectionType.value === 'Single') {
      // Strict single-domain mode: always replace previous selection.
      selectedDomainGaugeMap.value = {
        [domainCode]: nextGaugeIds
      }
      selectedDomainCodes.value = [domainCode]
      selectedGaugeIds.value = nextGaugeIds
      return
    }

    if (nextMap[domainCode]) {
      delete nextMap[domainCode]
    } else {
      nextMap[domainCode] = nextGaugeIds
    }
    selectedDomainGaugeMap.value = nextMap
    selectedDomainCodes.value = Object.keys(nextMap)
    selectedGaugeIds.value = [...new Set(Object.values(nextMap).flat().filter(Boolean))]
  }

  async function downloadSelectedData({
    startDate,
    endDate,
    waterQualityVariables,
    otherDatasets
  }) {
    ensureValidSession()
    if (selectedGaugeIds.value.length === 0) {
      throw new Error('Select at least one gauge before downloading.')
    }

    loading.value.download = true
    try {
      const payload = {
        gauges: selectedGaugeIds.value,
        start_date: `${startDate}T00:00:00Z`,
        end_date: `${endDate}T23:59:59Z`,
        water_quality_variables: waterQualityVariables,
        other_datasets: otherDatasets
      }

      const response = await fetch(ENDPOINTS.streamsDownload, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Streams-Session': sessionToken.value
        },
        body: JSON.stringify(payload)
      })

      if (!response.ok) {
        let detail = 'Download failed'
        try {
          const data = await response.json()
          detail = data.detail || detail
          if (data?.detail === 'Invalid or expired STREAMS session token.') {
            clearSession()
            throw new Error('Your STREAMS session expired. Please sign in again.')
          }
        } catch {
          // no-op
        }
        throw new Error(detail)
      }

      return await response.blob()
    } finally {
      loading.value.download = false
    }
  }

  function requestZoomToSelection() {
    zoomToSelectionRequestId.value += 1
  }

  function selectedGaugeFeatures() {
    const featureCollection = gaugesGeoJson.value
    if (!featureCollection || !Array.isArray(featureCollection.features)) {
      return []
    }
    const selected = new Set(selectedGaugeIds.value)
    return featureCollection.features.filter((feature) =>
      selected.has(feature?.properties?.STREAM_ID)
    )
  }

  return {
    sessionToken,
    sessionExpiresAt,
    options,
    gaugesGeoJson,
    selectedGaugeIds,
    selectedDomainCodes,
    selectedDomainGaugeMap,
    zoomToSelectionRequestId,
    selectionBy,
    selectionType,
    loading,
    login,
    logout,
    fetchOptions,
    fetchGauges,
    setSelectionBy,
    setSelectionType,
    clearSelectedGauges,
    clearSelectedDomains,
    clearSelections,
    toggleGaugeSelection,
    setSelectedGauges,
    toggleDomainSelection,
    downloadSelectedData,
    clearSession,
    requestZoomToSelection,
    selectedGaugeFeatures
  }
})
