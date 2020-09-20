<template>
  <section>
    <b-container class="p-0">
      <Header class="Small shadow" />
      <b-row class="mx-0 my-3" v-if="!showWorkArea">
        <b-col lg="12" class="mb-2">
          <b-form-file id="fileUploadButton" ref="file-input" accept=".xlsx, .xls, .csv" size="sm" class="d-none"
            @input="readFileInput"></b-form-file>
          <b-button variant="outline-dark" @click="fetchFileInput" :disabled="isFileLoading">
            <i class="fas fa-paperclip"></i>
          </b-button>
          <b-button variant="outline-dark">
            <b-icon icon="cloud-download"></b-icon>
          </b-button>
          <b-button variant="outline-dark">
            <i class="fas fa-database"></i>
          </b-button>
        </b-col>
        <b-col lg="12">
          <AlertMessage :alertMsg="alertMsg" />
          <b-table :items="fileDetails" :busy="isFileLoading" head-variant="dark" sticky-header striped hover outlined>
            <template v-slot:table-busy>
              <div class="text-center text-danger my-2">
                <b-spinner class="align-middle"></b-spinner>
                <strong>Loading...</strong>
              </div>
            </template>
          </b-table>
          <br />
          <b-button variant="dark" @click="ProcessToNext" :disabled="isFileLoading">Process</b-button>
        </b-col>
      </b-row>

      <div v-else>
        <b-row class="mx-0 my-3">
          <b-col lg="3" class="border-right border-dark">
            <b-form-group class="custom-border px-2">
              <label class="common_label">Choose Fileds</label>
              <b-form-checkbox-group v-model="selectedField" stacked>
                <draggable class="scrollbar" :list="Object.keys(primaryColumns)"
                  :group="{ name: 'dnd', pull: 'clone', put: true }" :clone="clone" @change="dndLogs">
                  <b-form-checkbox v-for="(opt, idx) in Object.keys(primaryColumns)" :key="idx" :handle="'.handle'+idx"
                    :value="opt">
                    {{opt}}
                  </b-form-checkbox>
                </draggable>
              </b-form-checkbox-group>
            </b-form-group>
            <b-row>
              <b-col class="p-1 bg_c">
                <label class="common_label m-0">Columns</label>
                <draggable class="drag_min custom-border" :list="dndColumns" :group="{ name: 'dnd', pull: true }"
                  @change="dndLogs">
                  <div class="span_items_list" v-for="(opt, idx) in dndColumnList" :key="idx">
                    {{ opt }}
                    <b-icon class="float-right my-1" icon="x" @click="removedndColumns(idx)" style="cursor: pointer;" />
                  </div>
                </draggable>
              </b-col>

              <b-col class="p-1 bg_c">
                <label class="common_label m-0"> Filters</label>
                <draggable class="drag_min custom-border" :list="dndFilters" :group="{ name: 'dnd', pull: true }"
                  @change="dndLogs">
                  <div class="span_items_list" v-for="(opt, idx) in dndFiltersList" :key="idx">
                    {{ opt }}
                    <b-icon class="float-right my-1" icon="x" @click="removedndFilters(idx)" />
                  </div>
                </draggable>
              </b-col>
            </b-row>

            <b-row>
              <b-col class="p-1  bg_c">
                <label class="common_label m-0">Values</label>
                <draggable class="drag_min custom-border" :list="dndValues" :group="{ name: 'dnd', pull: true }"
                  @change="dndLogs">
                  <div class="span_items_list" v-for="(opt, idx) in dndValuesList" :key="idx">
                    {{ opt }}
                    <b-icon class="float-right my-1" icon="x" @click="removedndValues(idx)" />
                  </div>
                </draggable>
              </b-col>

              <b-col class="p-1 bg_c">
                <label class="common_label m-0">Rows</label>
                <draggable class="drag_min custom-border" :list="dndRows" :group="{ name: 'dnd', pull: true }"
                  @change="dndLogs">
                  <div class="span_items_list" v-for="(opt, idx) in dndRowsList" :key="idx">
                    {{ opt }}
                    <b-icon class="float-right my-1" icon="x" @click="removedndRows(idx)" />
                  </div>
                </draggable>
              </b-col>
            </b-row>
          </b-col>

          <b-col lg="9">
            <AlertMessage :alertMsg="alertMsg" />
            <div class="border border-dark p-2 results d-flex justify-content-center align-items-center">

              <div class="w-100">
                <b-row>
                  <b-col lg="4">
                    <b-form-group label-cols="3" label-cols-lg="3" v-for="(name, val, idx) in SummaryFilters" :key="idx"
                      :label="val">
                      <b-dropdown ref="chkFilterDropdown" split-variant="outline-dark" variant="dark">
                        <b-dropdown-form style="width: 300px;height: 250px;overflow: auto;">
                          <b-form-checkbox v-model="chkFilterAllSelected" @change="toggleAll(name, val, 'Filters')">
                            {{ chkFilterAllSelected ? 'Un-select All' : 'Select All' }}
                          </b-form-checkbox>
                          <b-form-checkbox-group v-model="chkFilterSelected" stacked @change="submitFilters(val, 'Filters')">
                            <b-form-checkbox v-for="(opt, i) in name" :key="i" :value="opt">{{opt}}</b-form-checkbox>
                          </b-form-checkbox-group>
                        </b-dropdown-form>
                      </b-dropdown>
                    </b-form-group>

                    <b-form-group label-cols="3" label-cols-lg="3" v-for="(name, val, idx) in SummaryColumns" :key="idx"
                      :label="val">
                      <b-dropdown ref="chkColumnDropdown" split-variant="outline-dark" variant="dark">
                        <b-dropdown-form style="width: 300px;height: 250px;overflow: auto;">
                          <b-form-checkbox v-model="chkColumnAllSelected" @change="toggleAll(name, val, 'Columns')">
                            {{ chkColumnAllSelected ? 'Un-select All' : 'Select All' }}
                          </b-form-checkbox>
                          <b-form-checkbox-group v-model="chkColumnSelected" stacked @change="submitFilters(val, 'Columns')">
                            <b-form-checkbox v-for="(opt, i) in name" :key="i" :value="opt">{{opt}}</b-form-checkbox>
                          </b-form-checkbox-group>
                        </b-dropdown-form>
                      </b-dropdown>
                    </b-form-group>
                  </b-col>
                </b-row>

                <b-row>
                  <b-col lg="12" class="d-flex">
                    <div class="custom-border w-25 p-2 mr-2 mb-3">
                      <b-dropdown ref="chkRowDropdown" v-for="(name, val, idx) in SummaryRows" :key="idx"
                        split-variant="outline-dark" block variant="dark" :text="val" class="m-2">
                        <b-dropdown-form style="width: 300px;height: 250px;overflow: auto;">
                          <b-form-checkbox v-model="chkRowAllSelected" @change="toggleAll(name, val, 'Rows')">
                            {{ chkRowAllSelected ? 'Un-select All' : 'Select All' }}
                          </b-form-checkbox>
                          <b-form-checkbox-group v-model="chkRowSelected" stacked @change="submitFilters(val, 'Rows')">
                            <b-form-checkbox v-for="(opt, i) in name" :key="i" :value="opt">{{opt}}</b-form-checkbox>
                          </b-form-checkbox-group>
                        </b-dropdown-form>
                      </b-dropdown>
                    </div>
                    <b-table show-empty :items="SummaryValues" head-variant="dark" :busy="isBusy" responsive
                      sticky-header striped hover outlined>
                      <template v-slot:table-busy>
                        <div class="text-center text-danger my-2">
                          <b-spinner class="align-middle"></b-spinner>
                          <strong>Loading...</strong>
                        </div>
                      </template>

                      <template v-slot:empty="scope">
                        <div class="text-center"><strong>{{ scope.emptyText }}</strong></div>
                      </template>

                    </b-table>
                  </b-col>
                </b-row>
              </div>
            </div>

            <div class="w-100 text-right">
              <b-button variant="dark" class="m-2" @click="downloadCSV" :disabled="isDownloadBusy">
                <span v-show="!isDownloadBusy">Download CSV </span>
                <span v-show="isDownloadBusy">
                  <b-spinner small></b-spinner> Please Wait...
                </span>
              </b-button>
              <b-button variant="dark" class="m-2 float-right btn btn-primary" v-b-modal.modal-prompt
                :disabled="isSaveResultBusy">Save Results</b-button>
            </div>
          </b-col>
        </b-row>
      </div>
    </b-container>

    <!-- Save Workspace Modal Dialog -->
    <b-modal id="modal-prompt" title="Enter Workspace Name" @show="resetModal" @hidden="resetModal" @ok="saveResults">
      <b-form-group label="Workspace Name">
        <b-form-input v-model="workSpaceName"></b-form-input>
      </b-form-group>
    </b-modal>
  </section>
</template>

<script>
  import Header from "~/components/Header"
  import AlertMessage from "~/components/AlertMessage"
  import draggable from "vuedraggable"
  import XLSX from "xlsx";
  import path from 'path';
  export default {
    components: {
      Header,
      draggable,
      AlertMessage
    },
    data: () => {
      return {
        isFileLoading: false,
        isSaveResultBusy: false,
        isDownloadBusy: false,
        isBusy: false,
        alertMsg: {
          text: "",
          status: false
        },
        fileDetails: [],
        selectedField: [],
        showWorkArea: false,
        dndColumns: [],
        dndFilters: [],
        dndRows: [],
        dndValues: [],
        controlOnStart: true,
        chkFilterSelected: [],
        chkColumnSelected: [],
        chkRowSelected: [],
        chkFilterAllSelected: false,
        chkColumnAllSelected: false,
        chkRowAllSelected: false,
        workSpaceName: "",
        columnLevelFilter: {},
        apiRes: {},
        savedChkList: []
      }
    },
    mounted() {
      console.log("params: ", this.$route);
      this.$store.dispatch('headerInfo');
      if (this.$route.query.workarea) {
        this.showWorkArea = this.$route.query.workarea;
        this.savedWorkspaceList.forEach(i => {
          if (i.workspace_id == this.$route.query.workspace_id) {
            this.apiRes = i;
          }
        });
        console.log("apiRes: ", this.apiRes);
        this.$store.commit("fileName", this.apiRes.file_name);
        this.fetchColumn(this.apiRes.file_name);
        this.getTableData({
          filters: this.apiRes.filter,
          columns: this.apiRes.columns,
          rows: this.apiRes.rows,
          values: this.apiRes.values
        });
        this.dndColumns = this.apiRes.columns;
        this.dndRows = this.apiRes.rows;
        this.dndValues = this.apiRes.values;
        this.dndFilters = this.distinctFilters(Object.keys(this.apiRes.filter));

        let columnchkList = this.dndColumns.map(item => {
            return this.apiRes.filter[item];
        });
        this.chkColumnSelected = [].concat.apply([], columnchkList).filter(i => i);
        console.log("chkColumnSelected: ", this.chkColumnSelected);
        
        // this.chkColumnSelected = this.apiRes.filters[this.dndColumns[0]]
        // .map(item => {
        //     return this.apiRes.filters[item];
        // });
        let rowchkList = this.dndRows.map(item => {
            return this.apiRes.filter[item];
        });
        this.chkRowSelected = [].concat.apply([], rowchkList).filter(i => i);
        console.log("chkRowSelected: ", this.chkRowSelected);

        let filterchkList = this.dndFilters.map(item => {
            return this.apiRes.filter[item];
        });
        this.chkFilterSelected = [].concat.apply([], filterchkList).filter(i => i);
        console.log("chkFilterSelected: ", this.chkFilterSelected)

        // this.chkFilterSelected = this.apiRes.filters[this.dndFilters[0]]
        // .map(item => {
        //     return this.apiRes.filters[item];
        // });
        
      }
    },
    watch: {
      // SummaryFilters(val) {
      //     console.log("watch savedChkList: ", val, this.savedChkList);
      //     this.chkSelected = this.savedChkList;
      // }
    },
    computed: {
      savedWorkspaceList() {
        return this.$store.state.headerInfo;
      },
      dndColumnList: {
        get: function () {
          return [...new Set(this.dndColumns)];
        },
        // set: function (val) {
        //     this.dndColumnList = val;
        // }
      },
      dndRowsList: {
        get: function () {
          return [...new Set(this.dndRows)];
        },
        // set: function (val) {
        //     this.dndRowsList = val;
        // }
      },
      dndFiltersList: {
        get: function () {
          return [...new Set(this.dndFilters)];
        },
        // set: function (val) {
        //     this.dndFiltersList = val;
        // }
      },
      dndValuesList: {
        get: function () {
          return [...new Set(this.dndValues)];
        },
        // set: function (val) {
        //     this.dndValuesList = val;
        // }
      },
      primaryColumns: {
        get: function () {
          return this.$store.state.primaryColumns;
        },
        set: function (val) {
          // console.log("val from primaryColumns setter: ", val);
          this.$store.commit("primaryColumns", val);
        }
      },
      SummaryFilters: {
        get: function () {
          return this.$store.state.SummaryFilters;
        },
        set: function (val) {
          // console.log("val from SummaryFilters setter: ", val);
          this.$store.commit("SummaryFilters", val);
        }
      },
      SummaryColumns: {
        get: function () {
          return this.$store.state.SummaryColumns;
        },
        set: function (val) {
          // console.log("val from SummaryColumns setter: ", val);
          this.$store.commit("SummaryColumns", val);
        }
      },
      SummaryRows: {
        get: function () {
          return this.$store.state.SummaryRows;
        },
        set: function (val) {
          // console.log("val from SummaryRows setter: ", val);
          this.$store.commit("SummaryRows", val);
        }
      },
      SummaryValues: {
        get: function () {
          return this.$store.state.SummaryValues;
        },
        set: function (val) {
          // console.log("val from SummaryValues setter: ", val);
          this.$store.commit("SummaryValues", val);
        }
      }
    },
    methods: {
        distinctFilters(filterArray) {
            console.log("filterArray: ", filterArray);
            console.log("dndColumns: ", this.dndColumns);
            console.log("dndRows: ", this.dndRows);
            
            let mergedArray = this.dndColumns.concat(this.dndRows);
            console.log("mergedArray: ", mergedArray);
            // filterArray.forEach((i, idx) => {
            //     mergedArray.forEach(item => {
            //         if(i == item) filterArray.splice(idx, 1)
            //     });
            // }); 

            // filterArray = ["Region", "job", "Country", "Item Type", "Sales Channel"];
            // mergedArray = ["Region", "Sales Channel", "job","name","Country", "Order Priority"]
            filterArray.forEach((item, idx) =>  {
             mergedArray.forEach(data => {
                if(data == item) {
                  filterArray = filterArray.filter(i => i != item);
                }
              });
            });
            console.log("filterArray: ", filterArray);
            // let colResFlattened = 
            // console.log("colResFlattened: ", colResFlattened);

            // let rowRes = colResFlattened.map(item => {
            //     return this.dndRows.filter(i => i != item);
            // });
            // let finalResFlattened = [].concat.apply([], filterArray).filter(i => i);
            // console.log("finalResFlattened: ", finalResFlattened);
            return [].concat.apply([], filterArray).filter(i => i);
        },
      toggleAll(arr, val, chkName) {
        console.log("toggle arr: ", arr);
        console.log("toggle val: ", val);
        console.log("chkColumnSelected: ", this.chkColumnSelected)
        if (chkName == 'Columns') {
            // if(chkName == 'Rows') { this.chkRowSelected = arr; }
            if(!this.chkColumnAllSelected) { this.chkColumnSelected = arr; }
            else { this.chkColumnSelected = []; }
            // if(chkName == 'Filters') { this.chkFilterSelected = arr; }
        //   this.chkSelected = arr;
        // } else {
        //     // if(chkName == 'Rows') { this.chkRowSelected = []; }
        //     if(chkName == 'Columns') { this.chkColumnSelected = []; }
        //     // if(chkName == 'Filters') { this.chkFilterSelected = []; }
        }

         if (chkName == 'Rows') {
            if(!this.chkRowAllSelected) { this.chkRowSelected = arr; }
            else { this.chkRowSelected = []; }
            // if(chkName == 'Columns') { this.chkColumnSelected = arr; }
            // if(chkName == 'Filters') { this.chkFilterSelected = arr; }
        //   this.chkSelected = arr;
        // } else {
        //     if(chkName == 'Rows') 
            // if(chkName == 'Columns') { this.chkColumnSelected = []; }
            // if(chkName == 'Filters') { this.chkFilterSelected = []; }
        }

         if (chkName == 'Filters') {
            // if(chkName == 'Rows') { this.chkRowSelected = arr; }
            // if(chkName == 'Columns') { this.chkColumnSelected = arr; }
            if(!this.chkFilterAllSelected) { this.chkFilterSelected = arr; }
            else { this.chkFilterSelected = []; }
        //   this.chkSelected = arr;
        // } else {
            // if(chkName == 'Rows') { this.chkRowSelected = []; }
            // if(chkName == 'Columns') { this.chkColumnSelected = []; }
            // if(chkName == 'Filters') 
        }


        this.submitFilters(val, chkName);
      },
      ProcessToNext() {
        this.showWorkArea = true;
        this.SummaryFilters = {};
        this.SummaryColumns = {};
        this.SummaryRows = {};
        this.SummaryValues = [];
      },
      async dndLogs(evt) {
        console.log(`dndLogs: ${JSON.stringify(evt)}`);
        
        if (evt.removed) {
          this.selectedField = await this.selectedField.map(i => i != evt.removed.element ? i : "").filter(i => i);
        } else if (evt.added) {
          this.selectedField = await [...new Set([...this.selectedField, evt.added.element])];
        }
        
        
        let emptyFilters = this.dndFilters.map(i => {
            return {[i]: []}
        });
        console.log("emptyFilters: ", emptyFilters);
        this.columnLevelFilter = Object.assign(this.columnLevelFilter, ...emptyFilters);
        console.log("columnLevelFilter: ", this.columnLevelFilter);
        this.getTableData({
          filters: Object.assign({}, ...emptyFilters),
          columns: this.dndColumnList,
          rows: this.dndRowsList,
          values: this.dndValuesList
        });
      },
      submitFilters(evt, chkName) {
          console.log("chkName: ", chkName);
          console.log("chkFilterSelected: ", this.chkFilterSelected);
        setTimeout(() => {
            // let filterObject = {};
            if(chkName == 'Columns') { this.columnLevelFilter = Object.assign(this.columnLevelFilter, {[evt]: this.chkColumnSelected})}
            if(chkName == 'Rows') { this.columnLevelFilter = Object.assign(this.columnLevelFilter, {[evt]: this.chkRowSelected})}
            if(chkName == 'Filters') {this.columnLevelFilter = Object.assign(this.columnLevelFilter, {[evt]: this.chkFilterSelected})}
            console.log("columnLevelFilter: ", this.columnLevelFilter);
            this.getTableData({
                filters: this.columnLevelFilter,
                columns: this.dndColumnList,
                rows: this.dndRowsList,
                values: this.dndValuesList
            });
        }, 500);
      },
      clearFilters(dropdownName) {
        setTimeout(() => {
          this.$refs[dropdownName][0].hide(true);
          this.chkColumnAllSelected = false;
          this.chkRowAllSelected = false;
          this.chkFilterAllSelected = false;
          this.chkRowSelected = [];
          this.chkColumnSelected = [];
          this.chkFilterSelected = [];
        });
      },
      removedndColumns(idx) {
        this.dndColumnList.splice(idx, 1);
        this.getTableData({
          filters: this.dndFiltersList.length > 0 ? Object.assign(this.columnLevelFilter, {
            [this.dndFiltersList[0]]: []
          }) : {},
          columns: this.dndColumnList,
          rows: this.dndRowsList,
          values: this.dndValuesList
        });
      },
      removedndFilters(idx) {
        this.dndFiltersList.splice(idx, 1);
        this.getTableData({
          filters: this.dndFiltersList.length > 0 ? Object.assign(this.columnLevelFilter, {
            [this.dndFiltersList[0]]: []
          }) : {},
          columns: this.dndColumnList,
          rows: this.dndRowsList,
          values: this.dndValuesList
        });
      },
      removedndRows(idx) {
        this.dndRowsList.splice(idx, 1);
        this.getTableData({
          filters: this.dndFiltersList.length > 0 ? Object.assign(this.columnLevelFilter, {
            [this.dndFiltersList[0]]: []
          }) : {},
          columns: this.dndColumnList,
          rows: this.dndRowsList,
          values: this.dndValuesList
        });
      },
      removedndValues(idx) {
        this.dndValuesList.splice(idx, 1);
        this.getTableData({
          filters: this.dndFiltersList.length > 0 ? Object.assign(this.columnLevelFilter, {
            [this.dndFiltersList[0]]: []
          }) : {},
          columns: this.dndColumnList,
          rows: this.dndRowsList,
          values: this.dndValuesList
        });
      },
      clone(value) {
        console.log(`clone: ${ value }`)
        return value;
      },
      fetchFileInput() {
        document.getElementById('fileUploadButton').click();
      },
      fetchColumn(fileName) {
        this.$axios({
          method: "POST",
          url: 'http://127.0.0.1:5000/file_source?metadata_ind=yes',
          data: {
            file_name: fileName
          },
          headers: {
            "content-type": "application/json",
            "Access-Control-Allow-Origin": "*"
          }
        }).then(response => {
          console.log("fetchColumn response: ", response);
          const res = response.data;
          this.primaryColumns = res.data;
        }).catch(err => {
          this.alertMsg.status = true;
          this.alertMsg.text = err;
        });
      },
      readFileInput(fileSelected) {
        this.isFileLoading = true;
        this.alertMsg.status = false;
        console.log("fileSelected: ", fileSelected)
        // let fileReader = new FileReader();

        // fileReader.onload = (e) => {
        //   this.isFileLoading = false;
        //   let data = e.target.result;
        //   console.log("data: ", data);
        //   let file = new Blob([data], {type: fileSelected.type});
        //   console.log("file: ", file);
        // };

        // fileReader.onerror = function(err) {
        //     console.log("Error: ", err);
        //     this.alertMsg.status = true;
        //     this.alertMsg.text = err;
        //     this.isFileLoading = false;
        // };
        // fileReader.readAsBinaryString(fileSelected);


        // const str = path.join(__dirname,'pivotfiles/') //__dirname+'pivotfiles/';
        //  var pathName = path.resolve(str).replace('///g','\\');
          // const str =`.${__dirname}pivotfiles/`;
          // debugger
        // console.log("__dirname: ", `.${__dirname}pivotfiles/`)

        //  var pathName = str.replace('/','\\',);
        // console.log(pathName);
        this.$store.commit("fileName", `C:\\pivotfiles\\${fileSelected.name}`)
        this.fetchColumn(`C:\\pivotfiles\\${fileSelected.name}`);
        this.$axios({
          method: "POST",
          url: 'http://127.0.0.1:5000/file_source',
          data: {
            file_name: `C:\\pivotfiles\\${fileSelected.name}`
          },
          headers: {
            "content-type": "application/json",
            "Access-Control-Allow-Origin": "*"
          }
        }).then(response => {
          this.isFileLoading = false;
          console.log("fileDetails response: ", response);
          let res = response.data;
          this.fileDetails = res.data;
        }).catch(err => {
          this.isFileLoading = false;
          this.alertMsg.status = true;
          this.alertMsg.text = err;
        });

        // console.log("fileSelected: ", fileSelected);
        // let fileReader = new FileReader();

        // fileReader.onload = (e) => {
        //     this.isFileLoading = false;
        //     let data = e.target.result;
        //     let workbook = XLSX.read(data, {type: 'binary'});
        //     // console.log("workbook: ", workbook)
        //     workbook.SheetNames.forEach(sheetName => {                    
        //         // Here is your object
        //         let XL_row_object = XLSX.utils.sheet_to_row_object_array(workbook.Sheets[sheetName]);
        //         // let json_object = JSON.stringify(XL_row_object);
        //         // console.log("json_object: ", json_object);
        //         this.fileDetails = XL_row_object;
        //         if(XL_row_object.length !== 0) {
        //             this.$store.commit("primaryColumns", Object.keys(XL_row_object[0]));
        //         }
        //     });
        // };

        // fileReader.onerror = function(err) {
        //     console.log("Error: ", err);
        //     this.alertMsg.status = true;
        //     this.alertMsg.text = err;
        //     this.isFileLoading = false;
        // };
        // fileReader.readAsBinaryString(fileSelected);
      },
      getTableData(reqBody) {
          console.log("reqBody: ", reqBody)
        this.isBusy = true;
        this.alertMsg.status = false;
        this.$axios({
          method: "POST",
          url: 'http://127.0.0.1:5000/file_source?pivot_ind=yes',
          data: {
            file_name: this.$store.state.fileName,
            filter: reqBody.filters ? reqBody.filters :  {},
            columns: reqBody.columns,
            rows: reqBody.rows,
            values: reqBody.values
          },
          headers: {
            "content-type": "application/json",
            "Access-Control-Allow-Origin": "*"
          }
        }).then(response => {
          this.isBusy = false;
        //   console.log("SummaryResults response: ", response.data);
          const res = response.data.data;
          this.$store.commit("SummaryFilters", res.filter);
          this.$store.commit("SummaryValues", res.data);
          this.$store.commit("SummaryRows", res.rows);
          this.$store.commit("SummaryColumns", res.columns);
        }).catch(err => {
          this.alertMsg.status = true;
          this.alertMsg.text = err;
          this.isBusy = false;
          this.$store.commit("SummaryValues", []);
        });
      },
      resetModal() {
        this.workSpaceName = ""
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
              file_name: this.$store.state.fileName,
              columns: this.dndColumnList,
              filter: this.columnLevelFilter,
              values: this.dndValuesList,
              rows: this.dndRowsList
            },
            headers: {
              "content-type": "application/json",
              "Access-Control-Allow-Origin": "*"
            }
          }).then(response => {
            this.isSaveResultBusy = false;
            console.log("saved file response: ", response);
            this.alertMsg.status = true;
            this.alertMsg.text = response.data.data;
            this.resetModal();
            this.$bvModal.hide('modal-prompt');
            this.$router.push('/');
          }).catch(err => {
            this.alertMsg.status = true;
            this.alertMsg.text = err;
            this.isSaveResultBusy = false;
          });
        } catch (err) {
          console.error("Error: ", err);
          this.alertMsg.status = true;
          this.alertMsg.text = err;
          this.isSaveResultBusy = false;
        }
      },
      downloadCSV() {
        this.isDownloadBusy = true;
        try {
          this.$axios({
            method: "POST",
            url: 'http://127.0.0.1:5000/file_source?pivot_ind=yes',
            data: {
              file_name: this.$store.state.fileName,
              download_ind: "yes",
              columns: this.dndColumnList,
              filter: this.dndFiltersList || this.columnLevelFilter,
              values: this.dndValuesList,
              rows: this.dndRowsList
            },
            headers: {
              "content-type": "application/json",
              "Access-Control-Allow-Origin": "*"
            }
          }).then(response => {
            this.isDownloadBusy = false;
            console.log("download file response: ", response);
            const res = response.data.data.data[0]
            this.alertMsg.status = true;
            this.alertMsg.text = `your file saved in ${res.download_file}`;

          }).catch(err => {
            this.alertMsg.status = true;
            this.alertMsg.text = err;
            this.isDownloadBusy = false;
          });
        } catch (err) {
          console.error("Error: ", err);
          this.alertMsg.status = true;
          this.alertMsg.text = err;
          this.isDownloadBusy = false;
        }
      }
    }
  }

</script>
<style>
  .text-center.loading_symbol {
    position: absolute;
    top: 20%;
    left: 50%;
    z-index: 999;
  }

  .container {
    max-width: 100% !important
  }

  .b-table-sticky-header {
    height: calc(100vh - 239px);
    border: 1px solid #ccc;
  }

  .scrollbar {
    min-height: 25vh;
    max-height: 30vh;
    overflow: auto;
  }

  .table .thead-light th {
    color: #495057;
    background-color: #17a2ba;
    border-color: #dee2e6;
    color: #fff;
    padding: 5px 20px;
  }

  .results {
    min-height: 400px;
  }

  .b-table-sticky-header {
    overflow-y: auto;
    max-height: calc(90vh - 240px);
  }

  .drag_min {
    height: 120px;
    overflow: auto;
    background: #fff;
  }

  .custom-border {
    border: 2px dashed #ccc !important;
  }

  .span_items_list {
    padding: 1px 10px;
    border-bottom: 1px solid #ccc;
  }

  .common_label {
    font-weight: bold;
  }

  .bg_c {
    background: #f5f5f5;
  }

</style>
