<template>
  <!-- Actual map component included in base App.vue to allow persistence -->
  <DataViewDrawer v-if="$route.meta.enableFeatureLayers !== false" />
  <StreamControls v-else />
</template>

<script setup>
import DataViewDrawer from '../components/DataViewDrawer.vue'
import StreamControls from '@/components/StreamControls.vue'
</script>
