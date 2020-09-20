<template>
  <section>
    <b-container class="container-fluid p-0">
        <Header class="Small shadow" style="" />
         
        <div style="padding: 10px;margin-top: 15px;">
          <AlertMessage :alertMsg="alertMsg" />
          <b-row class="my-2 px-3">
            <b-col lg="12" class="mb-4">
              <b-link href="/workspace" class="btn btn-dark">New WorkSpace</b-link>
            </b-col>
            <b-col lg="3">
              <b-jumbotron bg-variant="dark" text-variant="light" class="p-3">
                <h4>Last Saved</h4>
                <div class="pr-2">
                  <div v-for="opt in savedWorkspaceList" :key="opt.workspace_id">
                    <nuxt-link :to="'/workspace?workspace_id='+opt.workspace_id+'&workarea=true'" class="text-light my-1"> 
                      {{ opt.workspace_name }} 
                    </nuxt-link>
                  </div>
                </div>
              </b-jumbotron>
            </b-col>
          </b-row>
        </div>

    </b-container>
  </section>
</template>

<script>
import Header from "~/components/Header"
import AlertMessage from "~/components/AlertMessage"
export default {
  components: {
    Header,
    AlertMessage
  },
  data: () => {
    return {
      alertMsg: {
        status: false,
        text: ''
      }
    }
  },
  mounted() {
    this.$store.dispatch('headerInfo');
  },
  computed: {
    savedWorkspaceList() {
      return this.$store.state.headerInfo;
    }
  },
  methods: {}
}
</script>

<style scoped>
.container{
  max-width:100% !important
}
.b-table-sticky-header{
    height: calc(100vh - 239px);
    border: 1px solid #ccc;
}
.b-table-sticky-header {
    overflow-y: auto;
    max-height: calc(90vh - 240px);
}
</style>