<template>
    <b-alert :show="alertMsg.status" :variant="alertMsg.variant || 'info'" dismissible>
      {{ alertMsg.text }}
    </b-alert>
</template>
<script>
export default {
    props: {
        alertMsg: Object
    },
    data: () => {
        return {

        }
    },
    watch: {
        alertMsg(val) {
            console.log("watch alert: ", val)
        }
    }
}
</script>
