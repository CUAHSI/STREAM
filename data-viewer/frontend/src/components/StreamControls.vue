<template>
  <v-navigation-drawer location="right" width="390" permanent>
    <v-container class="py-4">
      <h3 class="text-h6 mb-3">STREAM</h3>

      <div v-if="!streamsStore.sessionToken">
        <p class="text-body-2 mb-3">
          Sign in with HydroShare to access STREAMS gauges and downloads.
        </p>
        <v-text-field
          v-model="username"
          label="HydroShare Username"
          density="compact"
          hide-details
        />
        <v-text-field
          v-model="password"
          label="HydroShare Password"
          type="password"
          density="compact"
          hide-details
          class="mt-2"
        />
        <v-btn
          class="mt-3"
          color="primary"
          block
          :loading="streamsStore.loading.login"
          :disabled="!username || !password"
          @click="onLogin"
        >
          Sign In
        </v-btn>
      </div>

      <div v-else>
        <v-alert type="success" variant="tonal" density="compact" class="mb-3">
          Connected to HydroShare
        </v-alert>
        <v-btn block variant="outlined" class="mb-4" @click="onLogout"> Sign Out </v-btn>

        <v-radio-group
          :model-value="streamsStore.selectionBy"
          inline
          label="Select By"
          @update:model-value="streamsStore.setSelectionBy($event)"
        >
          <v-radio label="Gauge" value="gauge" />
          <v-radio label="Domain (HUC10)" value="domain" />
        </v-radio-group>
        <p class="text-caption mb-2">
          {{
            streamsStore.selectionBy === 'domain'
              ? 'Mode: click HUC10 boundaries to select gauges by watershed domain.'
              : 'Mode: click individual gauges on the map to select them.'
          }}
        </p>

        <v-radio-group
          :model-value="streamsStore.selectionType"
          inline
          label="Selection Type"
          @update:model-value="streamsStore.setSelectionType($event)"
        >
          <v-radio label="Single" value="Single" />
          <v-radio label="Multiple" value="Multiple" />
        </v-radio-group>

        <v-text-field
          :model-value="selectedSummary"
          label="Selected Gauge(s)"
          readonly
          density="compact"
          class="mb-2"
        />
        <div class="d-flex flex-wrap ga-1 mb-2">
          <v-chip
            v-for="gaugeId in streamsStore.selectedGaugeIds"
            :key="gaugeId"
            size="small"
            color="primary"
            variant="tonal"
          >
            {{ gaugeId }}
          </v-chip>
        </div>
        <div v-if="streamsStore.selectionBy === 'domain'" class="d-flex flex-wrap ga-1 mb-2">
          <v-chip
            v-for="code in streamsStore.selectedDomainCodes"
            :key="code"
            size="small"
            color="secondary"
            variant="tonal"
          >
            HUC10 {{ code }}
          </v-chip>
        </div>
        <v-btn
          size="small"
          variant="text"
          class="mb-2"
          :disabled="streamsStore.selectedGaugeIds.length === 0"
          @click="streamsStore.requestZoomToSelection()"
        >
          Zoom To Selected
        </v-btn>
        <v-btn size="small" variant="text" class="mb-4" @click="streamsStore.clearSelections()">
          Clear Selection
        </v-btn>

        <v-text-field
          v-model="startDate"
          type="date"
          label="Start Date"
          density="compact"
          class="mb-2"
        />
        <v-text-field
          v-model="endDate"
          type="date"
          label="End Date"
          density="compact"
          class="mb-4"
        />

        <v-divider class="my-2" />
        <div class="d-flex align-center justify-space-between mb-2">
          <p class="text-subtitle-2 ma-0">Water Quality Variables</p>
          <div>
            <v-btn size="x-small" variant="text" @click="selectAllWaterQuality"> Select All </v-btn>
            <v-btn size="x-small" variant="text" @click="clearWaterQuality"> Deselect All </v-btn>
          </div>
        </div>
        <v-checkbox
          v-for="label in waterQualityVariableLabels"
          :key="label"
          v-model="selectedWaterQuality"
          :label="label"
          :value="label"
          density="compact"
          hide-details
        />

        <v-divider class="my-3" />
        <div class="d-flex align-center justify-space-between mb-2">
          <p class="text-subtitle-2 ma-0">Other Variables</p>
          <div>
            <v-btn size="x-small" variant="text" @click="selectAllDatasets"> Select All </v-btn>
            <v-btn size="x-small" variant="text" @click="clearDatasets"> Deselect All </v-btn>
          </div>
        </div>
        <v-checkbox
          v-for="label in streamsStore.options.other_datasets"
          :key="label"
          v-model="selectedDatasets"
          :label="label"
          :value="label"
          density="compact"
          hide-details
        />

        <v-btn class="mt-2" variant="outlined" block @click="resetOptions"> Reset Options </v-btn>

        <v-btn
          class="mt-4"
          color="primary"
          block
          :loading="streamsStore.loading.download"
          :disabled="!canDownload"
          @click="onDownload"
        >
          Download Data
        </v-btn>
      </div>
    </v-container>
  </v-navigation-drawer>
</template>

<script setup>
import { computed, ref } from 'vue'
import { useStreamsStore } from '@/stores/streams'
import { useAlertStore } from '@/stores/alerts'

const streamsStore = useStreamsStore()
const alertStore = useAlertStore()

const username = ref('')
const password = ref('')
const selectedWaterQuality = ref([])
const selectedDatasets = ref([])
const startDate = ref('2011-01-01')
const endDate = ref(new Date().toISOString().slice(0, 10))

const waterQualityVariableLabels = computed(() =>
  Object.keys(streamsStore.options.water_quality_variables || {})
)

const selectedSummary = computed(() => {
  if (streamsStore.selectedGaugeIds.length === 0) return 'No Gauge Selected'
  return streamsStore.selectedGaugeIds.join(';')
})

const canDownload = computed(() => {
  return (
    streamsStore.selectedGaugeIds.length > 0 &&
    (selectedWaterQuality.value.length > 0 || selectedDatasets.value.length > 0) &&
    !!startDate.value &&
    !!endDate.value
  )
})

function selectAllWaterQuality() {
  selectedWaterQuality.value = [...waterQualityVariableLabels.value]
}

function clearWaterQuality() {
  selectedWaterQuality.value = []
}

function selectAllDatasets() {
  selectedDatasets.value = [...(streamsStore.options.other_datasets || [])]
}

function clearDatasets() {
  selectedDatasets.value = []
}

function resetOptions() {
  clearWaterQuality()
  clearDatasets()
  streamsStore.setSelectionBy('gauge')
  streamsStore.setSelectionType('Single')
  startDate.value = '2011-01-01'
  endDate.value = new Date().toISOString().slice(0, 10)
}

async function onLogin() {
  try {
    await streamsStore.login(username.value, password.value)
    password.value = ''
    alertStore.displayAlert({
      title: 'STREAMS',
      text: 'Signed in and gauges loaded.',
      type: 'success',
      closable: true,
      duration: 3
    })
  } catch (error) {
    alertStore.displayAlert({
      title: 'STREAMS Login Error',
      text: String(error),
      type: 'error',
      closable: true,
      duration: 6
    })
  }
}

async function onLogout() {
  await streamsStore.logout()
  selectedWaterQuality.value = []
  selectedDatasets.value = []
  alertStore.displayAlert({
    title: 'STREAMS',
    text: 'Signed out.',
    type: 'info',
    closable: true,
    duration: 3
  })
}

async function onDownload() {
  try {
    const blob = await streamsStore.downloadSelectedData({
      startDate: startDate.value,
      endDate: endDate.value,
      waterQualityVariables: selectedWaterQuality.value,
      otherDatasets: selectedDatasets.value
    })
    const url = window.URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = 'streams-data.zip'
    anchor.click()
    window.URL.revokeObjectURL(url)
  } catch (error) {
    alertStore.displayAlert({
      title: 'STREAMS Download Error',
      text: String(error),
      type: 'error',
      closable: true,
      duration: 6
    })
  }
}
</script>
