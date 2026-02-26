<template>
  <div v-show="$route.meta.showMap" id="mapContainer" />
  <v-card
    v-if="
      $route.meta.showMap &&
      $route.meta.enableFeatureLayers !== false &&
      zoom < minReachSelectionZoom
    "
    id="zoomIndicator"
    color="info"
    density="compact"
    dense
  >
    <v-card-text> <v-icon :icon="mdiMagnifyPlus" /> Zoom in to select reaches </v-card-text>
  </v-card>
  <v-card v-if="$route.meta.showMap" id="mouseposition" color="info">
    <v-card-text>
      <v-icon :icon="mdiCrosshairsGps" /> {{ latLong.lat?.toFixed(5) }},
      {{ latLong.lng?.toFixed(5) }} <br />
    </v-card-text>
  </v-card>
</template>

<script setup>
import 'leaflet/dist/leaflet.css'
import 'leaflet-easybutton/src/easy-button.css'
import L from 'leaflet'
import * as esriLeaflet from 'esri-leaflet'
import * as esriLeafletGeocoder from 'esri-leaflet-geocoder'
// import * as esriLeafletVector from 'esri-leaflet-vector';
import 'leaflet-easybutton/src/easy-button'
import { onMounted, onUpdated, watch, ref } from 'vue'
import { useMapStore } from '@/stores/map'
import { useAlertStore } from '@/stores/alerts'
import { useFeaturesStore } from '@/stores/features'
import { useChartsStore } from '@/stores/charts'
import { useStreamsStore } from '@/stores/streams'
import { mdiMagnifyPlus, mdiCrosshairsGps } from '@mdi/js'
import { storeToRefs } from 'pinia'
import { useRouter, useRoute } from 'vue-router'

const mapStore = useMapStore()
const alertStore = useAlertStore()
const featureStore = useFeaturesStore()
const chartStore = useChartsStore()
const streamsStore = useStreamsStore()

const router = useRouter()
const route = useRoute()
const staticFeatureLayers = ref([])
const streamGaugesLayer = ref(null)
const watershedLayers = ref({
  huc6: null,
  huc10: null,
  selectedHuc6Layer: null
})
const huc10QueryCache = ref(new Map())
const huc10DetectedField = ref('')
const huc10QueryRequestId = ref(0)

const HUC6_LAYER_URL =
  'https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_Watersheds_01/MapServer/2'
const HUC10_LAYER_URL =
  'https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_Watersheds_01/MapServer/4'

const {
  mapObject,
  zoom,
  center,
  baselayers,
  activeBaseLayerName,
  overlays,
  activeOverlays,
  minReachSelectionZoom,
  reachesFeatures,
  lakesFeatures
} = storeToRefs(mapStore)
const { activeFeature } = storeToRefs(featureStore)
const accessToken =
  'AAPK7e5916c7ccc04c6aa3a1d0f0d85f8c3brwA96qnn6jQdX3MT1dt_4x1VNVoN8ogd38G2LGBLLYaXk7cZ3YzE_lcY-evhoeGX'

const latLong = ref({
  lat: 0,
  lng: 0
})

function isStreamRoute() {
  return route.name === 'stream'
}

const featureLayersEnabled = () => route.meta.enableFeatureLayers !== false

function addLayerIfMissing(leaflet, layer) {
  if (leaflet && layer && !leaflet.hasLayer(layer)) {
    leaflet.addLayer(layer)
  }
}

function removeLayerIfPresent(leaflet, layer) {
  if (leaflet && layer && leaflet.hasLayer(layer)) {
    leaflet.removeLayer(layer)
  }
}

function applyRouteMode() {
  const leaflet = mapObject.value.leaflet
  if (!leaflet) {
    return
  }

  if (featureLayersEnabled()) {
    staticFeatureLayers.value.forEach((layer) => addLayerIfMissing(leaflet, layer))
    activeOverlays.value.forEach((layerName) => {
      if (layerName in overlays.value) {
        addLayerIfMissing(leaflet, overlays.value[layerName])
      }
    })
    removeLayerIfPresent(leaflet, streamGaugesLayer.value)
    removeLayerIfPresent(leaflet, watershedLayers.value.huc6)
    removeLayerIfPresent(leaflet, watershedLayers.value.huc10)
    return
  }

  staticFeatureLayers.value.forEach((layer) => removeLayerIfPresent(leaflet, layer))
  Object.values(overlays.value).forEach((layer) => removeLayerIfPresent(leaflet, layer))
  if (isStreamRoute()) {
    addLayerIfMissing(leaflet, streamGaugesLayer.value)
    addLayerIfMissing(leaflet, watershedLayers.value.huc6)
    addLayerIfMissing(leaflet, watershedLayers.value.huc10)
    if (streamsStore.selectionBy === 'domain') {
      if (watershedLayers.value.huc10?.bringToFront) watershedLayers.value.huc10.bringToFront()
      if (watershedLayers.value.huc6?.bringToFront) watershedLayers.value.huc6.bringToFront()
    } else if (streamGaugesLayer.value?.bringToFront) {
      streamGaugesLayer.value.bringToFront()
    }
  } else {
    removeLayerIfPresent(leaflet, streamGaugesLayer.value)
    removeLayerIfPresent(leaflet, watershedLayers.value.huc6)
    removeLayerIfPresent(leaflet, watershedLayers.value.huc10)
  }
  featureStore.clearSelectedFeatures()
  chartStore.clearChartData()
}

function buildWatershedLayers() {
  if (watershedLayers.value.huc6 && watershedLayers.value.huc10) {
    return
  }

  const huc10Layer = L.geoJSON(null, {
    style: () => ({
      color: '#0a9396',
      weight: 1.3,
      fillColor: '#94d2bd',
      fillOpacity: 0.18
    }),
    onEachFeature: (feature, layer) => {
      layer.on('click', (e) => {
        const { code, name } = extractWatershedMetadata(feature?.properties || {}, 10)
        if (streamsStore.selectionBy === 'domain') {
          const gaugeIds = getGaugeIdsInLayer(layer)
          streamsStore.toggleDomainSelection(code, gaugeIds)
          syncHuc10SelectionStyles()
        }
        const html = `
          <div>
            <strong>HUC10</strong><br/>
            <strong>Code:</strong> ${code}<br/>
            <strong>Name:</strong> ${name}
          </div>
        `
        L.popup().setLatLng(e.latlng).setContent(html).openOn(mapObject.value.leaflet)
      })
    }
  })

  const huc6Layer = esriLeaflet.featureLayer({
    url: HUC6_LAYER_URL,
    simplifyFactor: 0.35,
    precision: 5,
    minZoom: 3,
    maxZoom: 10,
    style: () => ({
      color: '#005f73',
      weight: 2,
      fillColor: '#2a9d8f',
      fillOpacity: 0.08
    })
  })

  huc6Layer.on('click', async (e) => {
    const leaflet = mapObject.value.leaflet
    if (!leaflet || !e?.layer?.feature?.geometry) {
      return
    }

    leaflet.fitBounds(e.layer.getBounds(), { maxZoom: 8, padding: [20, 20] })
    const { code, name } = extractWatershedMetadata(e.layer.feature?.properties || {}, 6)
    const requestId = ++huc10QueryRequestId.value
    const html = `
      <div>
        <strong>HUC6</strong><br/>
        <strong>Code:</strong> ${code}<br/>
        <strong>Name:</strong> ${name}
      </div>
    `
    L.popup().setLatLng(e.latlng).setContent(html).openOn(leaflet)

    watershedLayers.value.selectedHuc6Layer = e.layer
    syncHuc6SelectionStyles()

    try {
      const featureCollection = await queryHuc10ByHuc6(e.layer.feature?.geometry, code)
      if (requestId !== huc10QueryRequestId.value || !isStreamRoute()) {
        return
      }

      huc10Layer.clearLayers()
      if (featureCollection?.features?.length) {
        const strictlyNested = featureCollection.features.filter((feature) => {
          const huc10 = normalizeHucCode(
            extractWatershedMetadata(feature?.properties || {}, 10).code
          )
          const huc6 = normalizeHucCode(code)
          if (!huc6) return true
          return huc10.startsWith(huc6)
        })
        huc10Layer.addData(strictlyNested)
        syncHuc10SelectionStyles()
      }
    } catch (error) {
      alertStore.displayAlert({
        title: 'Watershed Query Error',
        text: `Could not load HUC10 subwatersheds: ${error.message || error}`,
        type: 'error',
        closable: true,
        duration: 6
      })
    }
  })

  watershedLayers.value.huc6 = huc6Layer
  watershedLayers.value.huc10 = huc10Layer
}

function getGaugeIdsInLayer(layer) {
  const featureCollection = streamsStore.gaugesGeoJson
  if (!featureCollection?.features?.length || !layer) {
    return []
  }

  const geometry = layer?.feature?.geometry
  const bounds = layer.getBounds ? layer.getBounds() : null
  const ids = []
  for (const feature of featureCollection.features) {
    const coords = feature?.geometry?.coordinates
    const gaugeId = feature?.properties?.STREAM_ID
    if (!Array.isArray(coords) || coords.length < 2 || !gaugeId) continue
    const inside =
      pointInGeometry(coords, geometry) ||
      (bounds ? bounds.contains(L.latLng(Number(coords[1]), Number(coords[0]))) : false)
    if (inside) ids.push(gaugeId)
  }
  return [...new Set(ids)]
}

function pointInGeometry(pointCoords, geometry) {
  if (!geometry || !geometry.type) return false
  const [x, y] = pointCoords

  if (geometry.type === 'Polygon') {
    return pointInPolygonRings([x, y], geometry.coordinates)
  }

  if (geometry.type === 'MultiPolygon') {
    return geometry.coordinates.some((polyCoords) => pointInPolygonRings([x, y], polyCoords))
  }

  // ArcGIS-style polygon geometry from some services
  if (geometry.rings && Array.isArray(geometry.rings)) {
    return pointInPolygonRings([x, y], geometry.rings)
  }

  return false
}

function pointInPolygonRings(point, polygonRings) {
  if (!Array.isArray(polygonRings) || polygonRings.length === 0) return false
  const outerRing = polygonRings[0]
  if (!isPointInRing(point, outerRing)) return false
  for (let i = 1; i < polygonRings.length; i += 1) {
    if (isPointInRing(point, polygonRings[i])) return false
  }
  return true
}

function isPointInRing(point, ring) {
  let inside = false
  const [px, py] = point
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i]
    const [xj, yj] = ring[j]
    const intersects = yi > py !== yj > py && px < ((xj - xi) * (py - yi)) / (yj - yi || 1e-12) + xi
    if (intersects) inside = !inside
  }
  return inside
}

function syncHuc6SelectionStyles() {
  const huc6Layer = watershedLayers.value.huc6
  if (!huc6Layer || !huc6Layer.eachFeature) return

  huc6Layer.eachFeature((layer) => {
    layer.setStyle(
      watershedLayers.value.selectedHuc6Layer === layer
        ? {
            color: '#bb3e03',
            weight: 2.8,
            fillColor: '#ee9b00',
            fillOpacity: 0.12
          }
        : {
            color: '#005f73',
            weight: 2,
            fillColor: '#2a9d8f',
            fillOpacity: 0.08
          }
    )
  })
}

function syncHuc10SelectionStyles() {
  const huc10Layer = watershedLayers.value.huc10
  if (!huc10Layer || !huc10Layer.eachLayer) return

  const selectedCodes = new Set(
    streamsStore.selectedDomainCodes.map((code) => normalizeHucCode(code))
  )
  const isDomainMode = streamsStore.selectionBy === 'domain'

  huc10Layer.eachLayer((layer) => {
    const code = normalizeHucCode(
      extractWatershedMetadata(layer?.feature?.properties || {}, 10).code
    )
    const selected = isDomainMode && code && selectedCodes.has(code)
    layer.setStyle(
      selected
        ? {
            color: '#bb3e03',
            weight: 2.3,
            fillColor: '#ee9b00',
            fillOpacity: 0.2
          }
        : {
            color: '#0a9396',
            weight: 1.3,
            fillColor: '#94d2bd',
            fillOpacity: 0.18
          }
    )
  })
}

async function queryHuc10ByHuc6(huc6Geometry, huc6Code) {
  const normalizedCode = normalizeHucCode(huc6Code)
  if (normalizedCode && huc10QueryCache.value.has(normalizedCode)) {
    return huc10QueryCache.value.get(normalizedCode)
  }

  const fieldCandidates = ['HUC10', 'HUC_10', 'HUC', 'huc10', 'huc_code', 'HUC_CODE', 'HU_10']

  if (normalizedCode) {
    const orderedFieldCandidates = huc10DetectedField.value
      ? [huc10DetectedField.value, ...fieldCandidates.filter((f) => f !== huc10DetectedField.value)]
      : fieldCandidates

    for (const field of orderedFieldCandidates) {
      try {
        const collection = await runHuc10WhereQuery(field, normalizedCode)
        if (collection?.features?.length) {
          huc10DetectedField.value = field
          huc10QueryCache.value.set(normalizedCode, collection)
          return collection
        }
      } catch {
        // keep trying field candidates
      }
    }
  }

  const collection = await runHuc10IntersectsQuery(huc6Geometry)
  if (normalizedCode) {
    huc10QueryCache.value.set(normalizedCode, collection)
  }
  return collection
}

function runHuc10WhereQuery(field, huc6Code) {
  const clause = `${field} LIKE '${huc6Code}%'`
  return new Promise((resolve, reject) => {
    esriLeaflet
      .query({ url: HUC10_LAYER_URL })
      .where(clause)
      .run((error, result) => {
        if (error) reject(error)
        else resolve(result)
      })
  })
}

function runHuc10IntersectsQuery(huc6Geometry) {
  return new Promise((resolve, reject) => {
    esriLeaflet
      .query({ url: HUC10_LAYER_URL })
      .intersects(huc6Geometry)
      .run((error, collection) => {
        if (error) reject(error)
        else resolve(collection)
      })
  })
}

function normalizeHucCode(code) {
  if (code === null || code === undefined) return ''
  return String(code).replace(/\D/g, '')
}

function extractWatershedMetadata(properties, level) {
  const codeCandidates = [
    `HUC${level}`,
    `HUC_${level}`,
    `HU_${level}`,
    'HUC_CODE',
    'huc',
    'huc_code',
    'code'
  ]
  const nameCandidates = ['NAME', `HU_${level}_NAME`, `HUC${level}_NAME`, 'WBDNAME', 'name']

  const keys = Object.keys(properties || {})
  let code = ''
  let name = ''

  for (const key of codeCandidates) {
    if (properties?.[key]) {
      code = properties[key]
      break
    }
  }
  if (!code) {
    const fuzzyCodeKey = keys.find((key) => /huc|hu_?\d|code/i.test(key))
    code = fuzzyCodeKey ? properties[fuzzyCodeKey] : 'N/A'
  }

  for (const key of nameCandidates) {
    if (properties?.[key]) {
      name = properties[key]
      break
    }
  }
  if (!name) {
    const fuzzyNameKey = keys.find((key) => /name/i.test(key))
    name = fuzzyNameKey ? properties[fuzzyNameKey] : 'N/A'
  }

  return {
    code: String(code ?? 'N/A'),
    name: String(name ?? 'N/A')
  }
}

function buildStreamGaugeLayer() {
  const leaflet = mapObject.value.leaflet
  if (!leaflet || !streamsStore.gaugesGeoJson) {
    removeLayerIfPresent(leaflet, streamGaugesLayer.value)
    return
  }

  const selectedSet = new Set(streamsStore.selectedGaugeIds)
  const gaugesInteractive = streamsStore.selectionBy === 'gauge'
  const nextLayer = L.geoJSON(streamsStore.gaugesGeoJson, {
    pointToLayer: function (feature, latlng) {
      const streamId = feature?.properties?.STREAM_ID
      const selected = selectedSet.has(streamId)
      return L.circleMarker(latlng, {
        radius: selected ? 7 : 5,
        color: selected ? '#ff3b30' : '#1d4ed8',
        weight: selected ? 2 : 1,
        fillOpacity: 0.75,
        interactive: gaugesInteractive
      })
    },
    onEachFeature: function (feature, layer) {
      const streamId = feature?.properties?.STREAM_ID
      layer.on('click', () => {
        if (streamsStore.selectionBy !== 'gauge') return
        streamsStore.toggleGaugeSelection(streamId)
      })
      const siteName = feature?.properties?.['site name'] || 'Gauge'
      const source = feature?.properties?.source || 'N/A'
      layer.bindTooltip(`${siteName}<br/>${streamId || ''}<br/>Source: ${source}`, { sticky: true })
    }
  })

  removeLayerIfPresent(leaflet, streamGaugesLayer.value)
  streamGaugesLayer.value = nextLayer
  if (isStreamRoute()) {
    streamGaugesLayer.value.addTo(leaflet)
    if (streamsStore.selectionBy === 'domain') {
      if (watershedLayers.value.huc10?.bringToFront) watershedLayers.value.huc10.bringToFront()
      if (watershedLayers.value.huc6?.bringToFront) watershedLayers.value.huc6.bringToFront()
    } else if (streamGaugesLayer.value?.bringToFront) {
      streamGaugesLayer.value.bringToFront()
    }
  }
}

function zoomToSelectedStreamGauges() {
  const leaflet = mapObject.value.leaflet
  if (!leaflet || !isStreamRoute()) {
    return
  }
  const selectedFeatures = streamsStore.selectedGaugeFeatures()
  if (selectedFeatures.length === 0) {
    return
  }

  const latLngs = selectedFeatures
    .map((feature) => {
      const coordinates = feature?.geometry?.coordinates
      if (!Array.isArray(coordinates) || coordinates.length < 2) {
        return null
      }
      return L.latLng(Number(coordinates[1]), Number(coordinates[0]))
    })
    .filter((value) => !!value)

  if (latLngs.length === 0) {
    return
  }

  if (latLngs.length === 1) {
    leaflet.setView(latLngs[0], Math.max(leaflet.getZoom(), 12))
    return
  }

  leaflet.fitBounds(L.latLngBounds(latLngs), { padding: [30, 30], maxZoom: 12 })
}

onUpdated(async () => {
  try {
    if (router?.currentRoute?.value.meta.showMap && mapObject.value?.leaflet) {
      mapObject.value.leaflet.invalidateSize()
      if (activeFeature.value) {
        featureStore.selectFeature(activeFeature.value)
      }
      await router.isReady()
      mapStore.updateRouteAfterMapChange()
    }
  } catch (error) {
    console.error('Error in onUpdated:', error)
  }
})

onMounted(async () => {
  // Initial OSM tile layer
  const EsriWorldTopoMap = esriLeaflet.tiledMapLayer({
    url: 'https://services.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer'
  })

  var CartoDB_PositronNoLabels = L.tileLayer(
    'https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
    {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 20
    }
  )

  var CartoDB_DarkMatterNoLabels = L.tileLayer(
    'https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png',
    {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 20
    }
  )

  baselayers.value = {
    EsriWorldTopoMap,
    CartoDB: L.tileLayer(
      'https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}{r}.png',
      {
        attribution:
          '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="http://cartodb.com/attributions">CartoDB</a>',
        subdomains: 'abcd',
        maxZoom: 19
      }
    ),
    CartoDB_PositronNoLabels,
    CartoDB_DarkMatterNoLabels
  }

  mapStore.generateLakesFeatures()

  let url = 'https://arcgis.cuahsi.org/arcgis/services/SWOT/SWOT_pld_v202/MapServer/WmsServer?'
  const lakesWMS = L.tileLayer.wms(url, {
    layers: 0,
    transparent: 'true',
    format: 'image/png',
    minZoom: 0,
    maxZoom: 9
  })

  url =
    'https://tiles.arcgis.com/tiles/P3ePLMYs2RVChkJx/arcgis/rest/services/Esri_Hydro_Reference_Overlay/MapServer'
  // url = 'https://tiles.arcgis.com/tiles/P3ePLMYs2RVChkJx/arcgis/rest/services/Esri_Hydro_Reference_Labels/MapServer'

  let hydro = esriLeaflet.tiledMapLayer({
    url: url,
    layers: 0,
    transparent: 'true',
    format: 'image/png'
  })

  // add reaches layer to map
  url =
    'https://arcgis.cuahsi.org/arcgis/services/SWOT/world_SWORD_reaches_mercator_v17b/MapServer/WMSServer?'
  const reachesWMS = L.tileLayer.wms(url, {
    layers: 0,
    transparent: 'true',
    format: 'image/png',
    minZoom: 0,
    maxZoom: minReachSelectionZoom.value - 1
  })

  mapStore.generateReachesFeatures()

  // add nodes layer to map
  url =
    'https://arcgis.cuahsi.org/arcgis/services/SWOT/world_SWORD_nodes_mercator_v17b/MapServer/WMSServer?'
  L.tileLayer.wms(url, {
    layers: 0,
    transparent: 'true',
    format: 'image/png',
    minZoom: 12,
    maxZoom: 13
  })

  url =
    'https://arcgis.cuahsi.org/arcgis/rest/services/SWOT/world_SWORD_nodes_mercator/FeatureServer/0'
  const nodesFeatures = esriLeaflet.featureLayer({
    url: url,
    simplifyFactor: 0.35,
    precision: 5,
    minZoom: 13,
    maxZoom: 18
  })

  // add USGS gage layer to map
  url = 'http://arcgis.cuahsi.org/arcgis/services/NHD/usgs_gages/MapServer/WmsServer?'
  let gages = L.tileLayer.wms(url, {
    layers: 0,
    transparent: 'true',
    format: 'image/png',
    minZoom: 9,
    maxZoom: 18
    // BGCOLOR: '#f4d03f',
  })

  // layer toggling
  overlays.value = {
    'USGS Gages': gages,
    // "Lakes": lakes,
    Lakes: lakesFeatures.value,
    // "SWORD Reaches": reaches,
    // "SWORD Nodes": sword_nodes,
    Nodes: nodesFeatures,
    Esri_Hydro_Reference_Overlay: hydro
  }

  // check the query params and set the map center and zoom
  await router.isReady()
  const currentRoute = router.currentRoute.value
  mapStore.checkQueryParams(currentRoute)
  let leaflet = L.map('mapContainer').setView(center.value, zoom.value)
  // prevent panning outside of the single world bounds
  leaflet.setMaxBounds([
    [-90, -180],
    [90, 180]
  ])

  mapObject.value.leaflet = leaflet
  mapObject.value.hucbounds = []
  mapObject.value.popups = []
  mapObject.value.buffer = 20
  mapObject.value.huclayers = []
  mapObject.value.reaches = {}
  mapObject.value.bbox = [99999999, 99999999, -99999999, -99999999]
  //Remove the common zoom control and add it back later later
  leaflet.zoomControl.remove()

  let activeBaseLayer = baselayers.value[activeBaseLayerName.value]
  if (activeBaseLayer) {
    activeBaseLayer.addTo(leaflet)
  } else {
    EsriWorldTopoMap.addTo(leaflet)
    activeBaseLayerName.value = 'EsriWorldTopoMap'
  }

  staticFeatureLayers.value = [lakesWMS, reachesWMS, reachesFeatures.value, lakesFeatures.value]
  featureStore.checkQueryParams(currentRoute)
  buildWatershedLayers()

  // /*
  //  * LEAFLET BUTTONS
  //  */

  // Layer Control

  // Add method to layer control class to get active overlays
  L.Control.Layers.include({
    getActiveOverlays: function () {
      // create hash to hold all layers
      var control, layers
      layers = []
      control = this

      // loop thru all layers in control
      control._layers.forEach(function (obj) {
        var layerName

        // check if layer is an overlay
        if (obj.overlay) {
          // get name of overlay
          layerName = obj.name
          // store whether it's present on the map or not
          if (control._map.hasLayer(obj.layer)) {
            // append the layer name
            layers.push(layerName)
          }
        }
      })
      return layers
    }
  })

  const control = L.control.layers(baselayers.value, overlays.value).addTo(leaflet)

  applyRouteMode()
  buildStreamGaugeLayer()

  /*
   * LEAFLET EVENT HANDLERS
   */
  leaflet.on('click', function (e) {
    mapClick(e)
  })

  leaflet.addEventListener('mousemove', (e) => {
    let lat = Math.round(e.latlng.lat * 100000) / 100000
    let lng = Math.round(e.latlng.lng * 100000) / 100000
    latLong.value = {
      lat: lat,
      lng: lng
    }
  })

  reachesFeatures.value.on('click', async function (e) {
    const feature = e.layer.feature
    feature.feature_type = 'Reach'
    featureStore.clearSelectedFeatures()
    if (!featureStore.checkFeatureSelected(feature)) {
      // Only allow one feature to be selected at a time
      featureStore.selectFeature(feature)
    }
  })

  nodesFeatures.on('click', function (e) {
    const popup = L.popup()
    console.log('Selected node:', e.layer.feature.properties)
    const content = `
        <h3>${e.layer.feature.properties.river_name}</h3>
        <h4>Node ID: ${e.layer.feature.properties.node_id}</h4>
        <p>
            <ul>
                <li>SWORD Width: ${e.layer.feature.properties.width}</li>
                <li>SWORD WSE: ${e.layer.feature.properties.wse}</li>
                <li>SWORD Sinuosity: ${e.layer.feature.properties.sinuosity}</li>
                <li>SWOT Dist_out: ${e.layer.feature.properties.dist_out}</li>
            </ul>
        </p>
        `
    popup.setLatLng(e.latlng).setContent(content).openOn(leaflet)
  })

  lakesFeatures.value.on('click', async function (e) {
    const feature = e.layer.feature
    console.log('Selected lake:', feature)
    feature.feature_type = 'PriorLake'
    featureStore.clearSelectedFeatures()
    if (!featureStore.checkFeatureSelected(feature)) {
      // Only allow one feature to be selected at a time
      featureStore.selectFeature(feature)
    }
  })

  leaflet.on('moveend zoomend', function (e) {
    let centerObj = e.target.getCenter()
    center.value = {
      lat: centerObj.lat,
      lng: centerObj.lng
    }
    zoom.value = e.target._zoom
  })

  // handler for baselayer change
  leaflet.on('baselayerchange', function (e) {
    console.log('Base layer changed to: ' + e.name)
    activeBaseLayerName.value = e.name
  })

  // handler for overlay change
  leaflet.on('overlayadd overlayremove', function (e) {
    if (!featureLayersEnabled()) {
      if (e.type === 'overlayadd') {
        leaflet.removeLayer(e.layer)
      }
      return
    }

    console.log('Overlay change: ' + e.name)
    activeOverlays.value = control.getActiveOverlays()
  })

  // validate the map
  validate_bbox_size()

  const swotRiverNameMapServiceProvider = esriLeafletGeocoder.mapServiceProvider({
    label: 'River names',
    url: 'https://arcgis.cuahsi.org/arcgis/rest/services/SWOT/world_SWORD_reaches_mercator/MapServer',
    layers: [0],
    searchFields: ['river_name']
  })

  const swotReachServiceProvider = esriLeafletGeocoder.mapServiceProvider({
    label: 'Reach ID',
    url: 'https://arcgis.cuahsi.org/arcgis/rest/services/SWOT/world_SWORD_reaches_mercator/MapServer',
    layers: [0],
    searchFields: ['reach_id', 'rch_id_up', 'rch_id_dn']
  })

  const hucMapServiceProvider = esriLeafletGeocoder.mapServiceProvider({
    label: 'HUC 8',
    maxResults: 3,
    url: '	https://arcgis.cuahsi.org/arcgis/rest/services/hucs/HUC_8/MapServer',
    layers: [0],
    searchFields: ['name']
  })

  esriLeafletGeocoder.featureLayerProvider({
    url: 'https://arcgis.cuahsi.org/arcgis/rest/services/hucs/HUC_8/FeatureServer/0',
    searchFields: ['name'],
    label: 'Huc 8',
    //bufferRadius: 5000,
    formatSuggestion: function (feature) {
      return feature.properties.name
    }
  })

  const addressSearchProvider = esriLeafletGeocoder.arcgisOnlineProvider({
    apikey: accessToken,
    maxResults: 3,
    nearby: {
      lat: -33.8688,
      lng: 151.2093
    }
  })
  /**/

  esriLeafletGeocoder
    .geosearch({
      position: 'topleft',
      placeholder: 'Search for a location or feature',
      title: 'Enter an address, river name, reach ID, or HUC8',
      useMapBounds: false,
      expanded: true,
      providers: [
        swotRiverNameMapServiceProvider,
        swotReachServiceProvider,
        hucMapServiceProvider,
        addressSearchProvider
      ]
    })
    .addTo(leaflet)

  // add zoom control again they are ordered in the order they are added
  L.control
    .zoom({
      position: 'topleft'
    })
    .addTo(leaflet)

  // Erase
  L.easyButton(
    'fa-eraser',
    function () {
      clearSelection()
    },
    'clear selected features'
  ).addTo(leaflet)
})

watch(
  () => route.fullPath,
  () => {
    applyRouteMode()
    if (!isStreamRoute() && watershedLayers.value.huc10) {
      watershedLayers.value.huc10.clearLayers()
    }
  }
)

watch(
  () => streamsStore.gaugesGeoJson,
  () => {
    buildStreamGaugeLayer()
    applyRouteMode()
  }
)

watch(
  () => streamsStore.selectedGaugeIds.slice(),
  () => {
    buildStreamGaugeLayer()
  }
)

watch(
  () => [streamsStore.selectionBy, streamsStore.selectionType],
  () => {
    syncHuc6SelectionStyles()
    syncHuc10SelectionStyles()
    buildStreamGaugeLayer()
  }
)

watch(
  () => streamsStore.selectedDomainCodes.slice(),
  () => {
    syncHuc10SelectionStyles()
  }
)

watch(
  () => streamsStore.zoomToSelectionRequestId,
  () => {
    zoomToSelectedStreamGauges()
  }
)

// async function getGageInfo(e) {
//   // TESTING GAGE INFO BOX
//   // quick and dirty buffer around cursor
//   // bbox = lon_min, lat_min, lon_max, lat_max
//   let buf = 0.001

//   let buffered_bbox =
//     e.latlng.lat -
//     buf +
//     ',' +
//     (e.latlng.lng - buf) +
//     ',' +
//     (e.latlng.lat + buf) +
//     ',' +
//     (e.latlng.lng + buf)
//   let defaultParameters = {
//     service: 'WFS',
//     request: 'GetFeature',
//     bbox: buffered_bbox,
//     typeName: 'usgs_gages:usgs_gages_4326',
//     SrsName: 'EPSG:4326',
//     outputFormat: 'ESRIGEOJSON'
//   }
//   let root = 'https://arcgis.cuahsi.org/arcgis/services/NHD/usgs_gages/MapServer/WFSServer'
//   let parameters = L.Util.extend(defaultParameters)
//   let gageURL = root + L.Util.getParamString(parameters)

//   let gage_meta = {}
//   console.log(gageURL)
//   let resp = await fetch(gageURL)
//   if (resp.ok) {
//     try {
//       let response = await resp.json()
//       if (response.features.length > 0) {
//         let atts = response.features[0].attributes
//         let geom = response.features[0].geometry
//         gage_meta.name = atts.STATION_NM
//         gage_meta.num = atts.SITE_NO
//         gage_meta.x = geom.x
//         gage_meta.y = geom.y
//       }
//     } catch (e) {
//       console.error('Error attempting json parse', e)
//     }
//   }
//   return gage_meta
// }

/**
 * Handles the click event on the map.
 *
 * @param {MouseEvent} e - The click event object.
 * @returns {Promise<void>} - A promise that resolves when the click event is handled.
 */
async function mapClick() {
  return

  // // exit early if not zoomed in enough.
  // // this ensures that objects are not clicked until zoomed in
  // let zoom = e.target.getZoom()
  // if (zoom < mapObject.value.selectable_zoom) {
  //   return
  // }

  // // check if gage was clicked
  // let gage = await getGageInfo(e)

  // // if a gage was selected, create a pop up and exit early.
  // // we don't want to toggle HUC selection if a gage was clicked
  // if (Object.keys(gage).length > 0) {
  //   // create map info object here

  //   // close all popups
  //   if (mapObject.value.popups.length > 0) {
  //     mapObject.value.leaflet.closePopup()
  //   }

  //   // create new popup containing gage info
  //   L.popup()
  //     .setLatLng([gage.y, gage.x])
  //     .setContent('<b>ID:</b> ' + gage.num + '<br>' + '<b>Name</b>: ' + gage.name + '<br>')
  //     //		             + '<b>Select</b>: <a onClick=traceUpstream("'+gage.num+'")>upstream</a>')
  //     .openOn(mapObject.value.leaflet)

  //   // exit function without toggling HUC
  //   return
  // }
}

function clearSelection() {
  // TODO: update clear selection function
  // Clears the selected features on the map

  for (let key in mapObject.value.hucbounds) {
    // clear the huc boundary list
    delete mapObject.value.hucbounds[key]

    // clear the polygon overlays
    mapObject.value.huclayers[key].clearLayers()
    delete mapObject.value.huclayers[key]

    // clear the hucs in the html template
  }

  featureStore.clearSelectedFeatures()
  chartStore.clearChartData()

  // update the map
  updateMapBBox()

  // clear and update the HUC textbox
  // document.querySelector('.mdl-textfield').MaterialTextfield.change('');
  alertStore.displayAlert({
    title: 'Cleared',
    text: 'Your map selection was cleared',
    type: 'info',
    closable: true,
    duration: 1
  })
}

/**
 * Calculates and draws the bounding box on the map.
 */
function updateMapBBox() {
  // calculate global boundary
  let xmin = 9999999
  let ymin = 9999999
  let xmax = -9999999
  let ymax = -9999999
  for (let key in mapObject.value.hucbounds) {
    let bounds = mapObject.value.hucbounds[key].getBounds()
    if (bounds.getWest() < xmin) {
      xmin = bounds.getWest()
    }
    if (bounds.getSouth() < ymin) {
      ymin = bounds.getSouth()
    }
    if (bounds.getEast() > xmax) {
      xmax = bounds.getEast()
    }
    if (bounds.getNorth() > ymax) {
      ymax = bounds.getNorth()
    }
  }

  // save the map bbox
  mapObject.value.bbox = [xmin, ymin, xmax, ymax]

  removeBbox()

  // redraw the bbox layer with new coordinates
  let coords = [
    [
      [xmin, ymin],
      [xmin, ymax],
      [xmax, ymax],
      [xmax, ymin],
      [xmin, ymin]
    ]
  ]
  let polygon = [
    {
      type: 'Polygon',
      coordinates: coords
    }
  ]

  // todo: add function to validate bbox and return back styling
  // check bbox area bounds
  let bbox = validate_bbox_size()

  let json_polygon = L.geoJSON(polygon, { style: bbox.style })

  // save the layer
  mapObject.value.huclayers['BBOX'] = json_polygon

  return bbox.is_valid
}

function removeBbox() {
  // remove the bbox layer if it exists
  if ('BBOX' in mapObject.value.huclayers) {
    // remove the polygon overlay
    mapObject.value.huclayers['BBOX'].clearLayers()
    delete mapObject.value.huclayers['BBOX']
  }
}

/**
 * Validates that size constraints for the subset bounding box
 * @returns {object} - bounding box style and is_valid flag
 */
function validate_bbox_size() {
  // todo: turn the bounding box red and deactivate the submit button.
  let bbox = mapObject.value.bbox

  let londiff = Math.abs(bbox[2] - bbox[0])
  let latdiff = Math.abs(bbox[3] - bbox[1])

  let sqr_deg = londiff * latdiff

  let valid = true
  if (bbox.includes(99999999) || bbox.includes(-99999999)) {
    valid = false
  }

  let style = {}
  if (sqr_deg < 4 && valid) {
    style = {
      fillColor: 'black',
      weight: 2,
      opacity: 1,
      color: 'green',
      fillOpacity: 0.01,
      lineJoin: 'round'
    }
  } else {
    style = {
      fillColor: 'black',
      weight: 2,
      opacity: 1,
      color: 'red',
      fillOpacity: 0.01,
      lineJoin: 'round'
    }
    valid = false
  }
  mapStore.boxIsValid = valid
  return { style: style, is_valid: valid }
}
</script>
<style scoped>
#mapContainer {
  width: 100%;
  height: 100%;
}

#zoomIndicator {
  position: fixed;
  bottom: 137px;
  left: 10px;
  z-index: 1000;
}

#mouseposition {
  position: absolute;
  bottom: 73px;
  left: 10px;
  padding: 1px;
  z-index: 1000;
}
</style>
