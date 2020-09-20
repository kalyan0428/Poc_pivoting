<template>
  <b-container fluid class="p-0" style="background-color: #fff;min-height: 100vh;">
    <Nuxt />
  </b-container>
</template>

<script>
  import Header from "~/components/Header"
  // import MainPage from "~/components/MainPage"
  import SideMenu from "~/components/SideMenu"
  export default {
    components: {
      Header,
      // MainPage,
      SideMenu
    },
    data: () => {
      return {
                
      }
    }
  }
</script>