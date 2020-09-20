<template>
  <section>
        <b-container class="p-0">
            <Header style="padding: 10px;" class="Small shadow"/>                
            <div style=";padding: 10px;margin-top: 15px;">
                <AlertMessage :alertMsg="alertMsg" />
                <b-row class="m-0" v-if="!showWorkArea">
                    <b-col lg="12" class="mb-2" >
                        <b-form-file id="fileUploadButton" ref="file-input"
                            accept=".xlsx, .xls, .csv"
                            size="sm" 
                            class="d-none"
                            @input="readFileInput"></b-form-file>
                        <b-button variant="outline-info" @click="fetchFileInput"
                            :disabled="isBusy">
                            <i class="fas fa-paperclip"></i>
                        </b-button>
                        <b-button variant="outline-info">
                            <b-icon icon="cloud-download"></b-icon>
                        </b-button>
                        <b-button variant="outline-info">
                            <i class="fas fa-database"></i>
                        </b-button>
                    </b-col>

                    <b-col lg="12" v-show="fileDetails.length !== 0">
                        <b-table striped hover :items="fileDetails" head-variant="light"
                            sticky-header :busy="isBusy"></b-table>
                        <br/>
                        <b-button variant="primary" @click="showWorkArea = true" :disabled="isBusy">Process</b-button>   
                    </b-col>
                </b-row>
                
                <div v-else>
                    <Draggable :columnNames="columnNames" :columnDatatypes="columnDatatypes" />  
                    <b-row class="m-0 d-none">
                        <b-col lg="3" class="border-right border-dark">
                            <b-form-group class="scrollbar">
                                <template v-slot:label>
                                    <b>Choose fields:</b><br>
                                    <b-form-checkbox
                                    v-model="allSelected"
                                    :indeterminate="indeterminate"
                                    @change="toggleAll"
                                    >
                                    {{ allSelected ? 'Un-select All' : 'Select All' }}
                                    </b-form-checkbox>
                                </template>

                                <b-form-checkbox-group
                                    v-model="selectedField"
                                    class="ml-4"
                                    aria-label="Individual flavours"
                                    stacked
                                >
                                    <b-form-checkbox :value="val" v-for="(val, idx) in fieldsSelected" :key="idx">
                                        {{ val }}
                                    </b-form-checkbox>
                                </b-form-checkbox-group>
                            </b-form-group>
                            <!-- <b-form-group label="Choose fields" class="scrollbar">
                                <b-form-checkbox-group
                                    v-model="selectedField"
                                    stacked>
                                    <b-form-checkbox :value="{name, val}" v-for="(name, val, idx) in fieldsArray" :key="idx">
                                        {{ val }}
                                    </b-form-checkbox>
                                </b-form-checkbox-group>
                            </b-form-group>  -->
                            <b-button variant="primary" class="btn btn-primary" @click="saveSelection" :disabled="isBusy">Submit</b-button>
                        </b-col>

                        <b-col lg="9">
                            <b-row>
                                <b-col>
                                    <b-form-group label="COLUMNS" label-for="input_columns">
                                        <!-- <div v-for="(opt, idx) in selectedField" :key="idx">
                                            {{ opt.val }}
                                        </div> -->
                                    </b-form-group>
                                </b-col>

                                <b-col>
                                    <b-form-group label="FILTER" label-for="input_columns">
                                        <!-- <div v-for="(opt, idx) in selectedField" :key="idx">
                                            {{ opt.name }}
                                        </div> -->
                                    </b-form-group>
                                </b-col>

                                <b-col>
                                    <b-form-group label="VALUES" label-for="input_columns">
                                        <!-- <div v-for="(opt, idx) in selectedField" :key="idx">
                                            {{ opt.name }}
                                        </div> -->
                                    </b-form-group>
                                </b-col>

                                <b-col>
                                    <b-form-group label="ROWS" label-for="input_columns">
                                        <!-- <div v-for="(opt, idx) in selectedField" :key="idx">
                                            {{ opt.name }}
                                        </div> -->
                                    </b-form-group>
                                </b-col>
                            </b-row>
							   <div v-if="isBusy" class="text-center loading_symbol">
                                    <b-spinner variant="primary" label="Spinning" />
                                </div>
                            <div class="border border-dark p-2 results d-flex justify-content-center align-items-center">
                                <span v-if="finalResults.length === 0">results will show here....</span>
                                <div class="w-100" v-else> 
                                    <b-table striped hover :items="finalResults" head-variant="light"
                                        sticky-header></b-table>
                                    <b-button :disabled="offset <= 1 || isBusy" @click="onPrev">Prev</b-button>
                                    <b-button :disabled="totalRows < (this.finalResults.length + 1) || isBusy" @click="onNext">Next</b-button>
                                </div>
                            </div>
                            <!-- <b-button variant="primary" class="m-2 float-right btn btn-primary" :disabled="isSaveResultBusy"
                                @click="downloadCSV">
                                    <b-icon icon="arrow-clockwise" animation="spin" font-scale="4"></b-icon> Download CSV
                            </b-button> -->

                            <div class="w-100 text-right">                                
                                <b-button variant="primary" class="m-2" @click="downloadCSV" :disabled="isDownloadBusy">
                                    <span v-show="!isDownloadBusy">Download CSV  </span>
                                    <span v-show="isDownloadBusy"><b-spinner small></b-spinner> Please Wait...</span>
                                </b-button>
                                <b-button variant="primary" class="m-2 float-right btn btn-primary" v-b-modal.modal-prompt :disabled="isSaveResultBusy">Save Results</b-button>
                            </div>
                        </b-col>
                    </b-row>
                </div>
            </div>
         
        </b-container>

        <b-modal id="modal-prompt"
            title="Enter Workspace Name"
            @show="resetModal"
            @hidden="resetModal"
            @ok="saveResults">
                <b-form-group label="Workspace Name">
                    <b-form-input v-model="workSpaceName"></b-form-input>
                </b-form-group>
            </b-modal>
  </section>
</template>

<script>
import Header from "~/components/Header"
import AlertMessage from "~/components/AlertMessage"
import Draggable from "~/components/Draggable"
import XLSX from "xlsx";

export default {
    components: {
        Header,
        Draggable,
        AlertMessage
    },
    data: () => {
        return {
            showWorkArea: false,
            fileDetails: [],
            selectedField: [],
            allSelected: false,
            indeterminate: false,
            columnArray: [],
            isDownloadBusy: false,
            filterArray: [],
            valueArray: [],
            rowArray: [],
            fieldsArray: {},
            columnNames: [],
            columnDatatypes: [],
            finalResults: [],
            workSpaceName: "",
            isBusy: false,
            alertMsg: {
                text: "",
                status: false
            },
            isSaveResultBusy: false,
            offset: 1,
            totalRows: 0
        }
    },
    mounted() {
        // console.log("xlsx: ", XLSX);
        this.offset = 1;
    },
    watch: {
        fieldsArray(val) {
            this.fieldsSelected = Object.keys(val);
        },
        selectedField(newVal, oldVal) {
            // Handle changes in individual flavour checkboxes
            console.log(newVal, oldVal)
            console.log("fieldsSelected: ", this.fieldsSelected)
            if (newVal.length === 0) {
                this.indeterminate = false
                this.allSelected = false
            } else if (newVal.length === this.fieldsSelected.length) {
                this.indeterminate = false
                this.allSelected = true
            } else {
                this.indeterminate = true
                this.allSelected = false
            }
        }
    },
    methods: {
        toggleAll(checked) {
            this.selectedField = checked ? this.fieldsSelected.slice() : []
        },
        fetchColumn(fileName){
            this.isBusy = true;
            this.$axios({
                method: "POST",
                url: 'http://127.0.0.1:5000/file_source?metadata_ind=yes',
                data: {
                    file_name: fileName
                },
                headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
            }).then(response => {
                this.isBusy = false;
                console.log("fetchColumn response: ", response);
                let res = response.data;
                this.fieldsArray = res.data;
                this.columnNames = Object.keys(res.data);
                this.columnDatatypes = Object.values(res.data);
            }).catch(err => {
                this.alertMsg.status = true;
                this.alertMsg.text = err;
                this.isBusy = false;
            });
        },
        readFileInput(fileSelected){
            this.isBusy = true;
            this.alertMsg.status = false;
            this.$store.commit("fileName", `.\\pivotfiles\\${fileSelected.name}`)
            this.fetchColumn(`.\\.pivotfiles\\${fileSelected.name}`);
            this.$axios({
                method: "POST",
                url: 'http://127.0.0.1:5000/file_source',
                data: {
                    file_name: `C:\\Users\\venkats_mandadapu\\Downloads\\${fileSelected.name}`
                },
                headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
            }).then(response => {
                this.isBusy = false;
                console.log("fileDetails response: ", response);
                let res = response.data;
                this.fileDetails = res.data;
            }).catch(err => {
                this.isBusy = false;
                this.alertMsg.status = true;
                this.alertMsg.text = err;
            });
        },
        fetchFileInput() {
            document.getElementById('fileUploadButton').click();
        },
        // async myProvider(ctx) {
        //     try {
        //         const response = await this.$axios.post(`http://127.0.0.1:5000/file_source?pivot_ind=yes&page=${ctx.currentPage}&size=${ctx.perPage}`)
        //         return response.items
        //     } catch (error) {
        //         return []
        //     }
        // },
        async saveSelection() { 
            this.isBusy = true; 
            this.offset = 1;
            await this.getTableData();
            // this.alertMsg.status = false;         
            // await this.$axios({
            //     method: "POST",
            //     url: 'http://127.0.0.1:5000/file_source?pivot_ind=yes',
            //     data: {
            //         file_name: this.$store.state.fileName,
            //         columns: this.selectedField.map(i => i.val),
            //         offset: this.offset
            //     },
            //     headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
            // }).then(response => {
            //     this.isBusy = false;                
            //     console.log("finalResults response: ", response.data);
            //     let res = response.data;
            //     this.finalResults = res.data;
            //     this.totalRows = res.total_rows
            // }).catch(err => {
            //     this.alertMsg.status = true;
            //     this.alertMsg.text = err;
            //     this.isBusy = false;
            // });
        },
        async getTableData() { 
            this.isBusy = true; 
            this.alertMsg.status = false;         
            await this.$axios({
                method: "POST",
                url: 'http://127.0.0.1:5000/file_source?pivot_ind=yes',
                data: {
                    file_name: this.$store.state.fileName,
                    columns: this.selectedField,
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
        },
        saveResults() {
            this.isSaveResultBusy = true;
            this.alertMsg.status = false;
            try {
                this.$axios({
                    method: "POST",
                    url: 'http://127.0.0.1:5000/workspaces',
                    data: {
                        workspace_name: this.workSpaceName || `Workspace_${Math.floor(Math.random() * 10000)}`,   
                        columns: this.selectedField,
                        filter: "", 
                        value:"",
                        rows:"",
                        data_source: "C:\\Users\\abhi\\emp.xlsx"
                    },
                    headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
                }).then(response => {
                    this.isSaveResultBusy = false;
                    console.log("saved file response: ", response);
                    this.resetModal();
                    this.$bvModal.hide('modal-prompt');
                    this.$router.push('/');
                }).catch(err => {
                    this.alertMsg.status = true;
                    this.alertMsg.text = err;
                    this.isSaveResultBusy = false;
                });
            } catch(err) {
                console.error("Error: ", err);
                this.alertMsg.status = true;
                this.alertMsg.text = err;
                this.isSaveResultBusy = false;
            }
        },
        handleOk() {
            this.saveResults();            
        },
        resetModal() {
            this.workSpaceName = ""
        },
        downloadCSV() {
            this.isDownloadBusy = true;
            try {
                this.$axios({
                    method: "POST",
                    url: 'http://127.0.0.1:5000/download',
                    data: {
                        file_name: this.$store.state.fileName,
                        columns: this.selectedField
                    },
                    headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
                }).then(response => {
                    this.isDownloadBusy = false;
                    console.log("download file response: ", response);
                    
                    if(response.data.status == "success") {
                        // file download
                        let link = document.createElement("a");
                        if (link.download !== undefined) { // feature detection
                            // Browsers that support HTML5 download attribute
                            // const url = URL.createObjectURL(response.data.download_file);
                            link.setAttribute("href", response.data.download_file);
                            link.setAttribute("download", 'download.csv');
                            link.style.visibility = 'hidden';
                            document.body.appendChild(link);
                            link.click();
                            document.body.removeChild(link);
                        }
                    } else {
                        // error
                    }
                }).catch(err => {
                    this.alertMsg.status = true;
                    this.alertMsg.text = err;
                    this.isDownloadBusy = false;
                });
            } catch(err) {
                console.error("Error: ", err);
                this.alertMsg.status = true;
                this.alertMsg.text = err;
                this.isDownloadBusy = false;
            }
        }
        // async readFileInput(fileSelected) {
        //     console.log("fileSelected: ", fileSelected);
        //     let fileReader = new FileReader();

        //     fileReader.onload = (e) => {
        //         let data = e.target.result;
        //         let workbook = XLSX.read(data, {type: 'binary'});
        //         // console.log("workbook: ", workbook)
        //         workbook.SheetNames.forEach(sheetName => {
        //             // Here is your object
        //             let XL_row_object = XLSX.utils.sheet_to_row_object_array(workbook.Sheets[sheetName]);
        //             // let json_object = JSON.stringify(XL_row_object);
        //             // console.log("json_object: ", json_object);
        //             // this.fileDetails = XL_row_object;
        //         });
        //     };

        //     fileReader.onerror = function(err) {
        //         console.log("Error: ", err);
        //     };
        //     fileReader.readAsBinaryString(fileSelected);
        // }
    }
}
</script>
<style>
.text-center.loading_symbol{
position:absolute;
top:20%;
left:50%;
z-index:999;
}

.container{
  max-width:100% !important
}
.b-table-sticky-header{
    height: calc(100vh - 239px);
    border: 1px solid #ccc;
}
.scrollbar{
    height: calc(100vh - 190px);
overflow: auto;
}
.table .thead-light th {
    color: #495057;
    background-color: #17a2ba;
    border-color: #dee2e6;
    color: #fff;
    padding: 5px 20px;
}
.results{
    min-height: 400px;
}
.b-table-sticky-header {
    overflow-y: auto;
    max-height: calc(90vh - 240px);
}
</style>

