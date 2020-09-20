<template>
    <section>
        <b-container fluid class="p-0">
            <Header style="padding: 10px;" class="Small shadow"/>                
            <div style=";padding: 10px;margin-top: 15px;">
                <AlertMessage :alertMsg="alertMsg" />
                <div class="w-100">
                    <Draggable :columnNames="workspaceSelected.columns ? Object.values(workspaceSelected.columns.split(':')) : []"
                        :finalResults="finalResults" />
                    <!-- {{workspaceSelected}}
                    <div> columns: {{workspaceSelected.columns}}</div>
                    <div>data_source: {{ workspaceSelected.data_source}}</div> -->
                </div>
            </div>
        </b-container>
    </section>
</template>

<script>
import Header from "~/components/Header"
import AlertMessage from "~/components/AlertMessage"
import Draggable from "~/components/Draggable"
export default {
    components: {
        Header,
        Draggable,
        AlertMessage
    },
    data: () => {
        return {
            isBusy: false,
            alertMsg: {
                text: "",
                status: false
            },
            workspaceSelected: {},
            offset: 1,
            totalRows: 0,
            finalResults: []
        }
    },
    async mounted() {
        console.log("params: ", this.$route.params.id);
        try {
            let apiResponse = await this.$axios({
                method: "GET",
                url: `http://127.0.0.1:5000/workspaces?id=${this.$route.params.id}`,
                data: {
                    id: this.$route.params.id
                },
                headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
            });
            console.log(`apiResponse: ${apiResponse}`);
            if(apiResponse.data.status == "success") {
                this.workspaceSelected = apiResponse.data.data[0];
                this.loadGrid();
            } else {
                throw apiResponse.data.data;
            }
        } catch(err) {
            console.error("Error: ", err);
        }
    },
    methods: {
        loadGrid() {
            this.offset = 1;
            this.getTableData();
        },
        async getTableData() { 
            this.isBusy = true; 
            this.alertMsg.status = false;         
            await this.$axios({
                method: "POST",
                url: 'http://127.0.0.1:5000/file_source?pivot_ind=yes',
                data: {
                    file_name: this.workspaceSelected.data_source,
                    columns: this.workspaceSelected.columns,
                    offset: this.offset
                },
                headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
            }).then(response => {
                this.isBusy = false;                
                console.log("finalResults response: ", response.data);
                let res = response.data;
                this.finalResults = res.data;
                this.totalRows = res.total_rows
            }).catch(err => {
                this.alertMsg.status = true;
                this.alertMsg.text = err;
                this.isBusy = false;
            });
        },
        onNext(){
            this.offset += 1;
            this.getTableData();
        },
        onPrev(){
            this.offset -= 1;
            this.getTableData();
        }
    }
}
</script>