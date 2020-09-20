<template>
<section>
    <!-- columns settings dialog -->
    <b-modal id="column-settings-dialog" ref="settingsModal" title="Settings" size="lg"
      @show="resetColumnDialog" @hidden="resetColumnDialog">
      <b-row>
        <b-col lg="5">
            <label>Choose fields:</label>
            <b-form-select multiple :select-size="15">
                <b-form-select-option v-for="(list, idx) in columnNameList" :key="idx" :value="list" >
                    {{ list }}
                </b-form-select-option>
            </b-form-select>
        </b-col>
        <b-col lg="2" class="d-flex justify-content-center align-items-center">
            <b-button-group vertical>
                <b-button class="my-2" expanded><i class="fas fa-angle-double-right"></i></b-button>
                <b-button class="my-2" expanded><span><i class="fas fa-angle-double-left"></i></span></b-button>
            </b-button-group>
        </b-col>

        <b-col lg="5">
            <label>Selected fields:</label>
            <b-form-select multiple :select-size="15">
                <b-form-select-option v-for="(list, idx) in selectedColumnList" :key="idx" :value="list" >
                    {{ list }}
                </b-form-select-option>
            </b-form-select>
        </b-col>
      </b-row>

        <template v-slot:modal-footer>
            <b-button @click="vaidateChanges">Validate</b-button>
            <b-button @click="submitChanges">Submit</b-button>
            <b-button @click="$bvModal.hide('column-settings-dialog')">Cancel</b-button>
        </template>  
    </b-modal>

    <!-- filter settings dialog -->
    <b-modal id="filter-settings-dialog" ref="FilterSettings" title="Settings"
      size="lg" @show="resetModal" @hidden="resetModal">
        <b-row>
            <b-col log="4">
                <b-form-group label="Column Names:">
                    <div class="border border-1 border-dark p-1">
                        <b-form-checkbox-group v-model="selectedColumnNames" stacked>
                            <b-form-checkbox :value="val" v-for="(name, val, idx) in columnNames" :key="idx">
                                {{ val }}
                            </b-form-checkbox>
                        </b-form-checkbox-group>
                    </div>
                </b-form-group>

                <b-form-group label="Analytical Functions:">
                    <div class="border border-1 border-dark p-1"> 
                        <b-button v-for="(btn, idx) in analytics" 
                            :key="idx"
                            :pressed.sync="btn.state"
                            class="mx-1 mb-1">
                            {{ btn.caption }}
                        </b-button>
                    </div>
                </b-form-group>

                <b-form-group label="Operators:">
                    <div class="border border-1 border-dark p-1">
                        <b-button v-for="(btn, idx) in operators" 
                            :key="idx"
                            :pressed.sync="btn.state"
                            class="mx-1 mb-1">
                            {{ btn.caption }}
                        </b-button>
                    </div>
                </b-form-group>
            </b-col>

            <b-col lg="8">
                <div class="border border-1 border-dark p-1">
                    <label>selectedColumnNames: </label> {{ selectedColumnNames }}
                    <br />
                    <label>selectedAnalytics: </label>{{ selectedAnalytics }}
                    <br />
                    <label>selectedOperators: </label>{{ selectedOperators }}
                </div>
            </b-col>
        </b-row>

        <template v-slot:modal-footer>
            <b-button @click="vaidateChanges">Validate</b-button>
            <b-button @click="submitChanges">Submit</b-button>
            <b-button @click="$bvModal.hide('modal-settings-dialog')">Cancel</b-button>
        </template>  
    </b-modal>
     
    </section>
</template>

<script>
export default {
    props: {
        
    },
    data: () => {
        return {
            columnNames: {
                "empno": "int64",
                "ename": "string",
                "job": "string",
                "mgr": "float64",
                "sal": "int64",
                "comm": "float64",
                "deptno": "int64"
            },
            analytics: [
                { caption: 'sum', state: false },
                { caption: 'average', state: false },
                { caption: 'min', state: false },
                { caption: 'max', state: false }
            ],
            operators: [
                { caption: '+', state: false },
                { caption: '-', state: false },
                { caption: '*', state: false },
                { caption: '/', state: false },
                { caption: '%', state: false }
            ],
            selectedColumnNames: []
        }
    },
    computed: {
        selectedOperators() {
            return this.operators.map(btn => {
                if(btn.state) return btn.caption;
            }).filter(i => i);
        },
        selectedAnalytics() {
            return this.analytics.map(btn => {
                if(btn.state) return btn.caption;
            }).filter(i => i);
        },
        columnNameList() {
            return this.$store.state.columnNameList;
        },
        selectedColumnList: {
            get: function() {
                return this.$store.state.selectedColumnList;
            },
            set: function (val) {
                this.$store.commit("selectedColumnList", val);
            }
        }
    },
    methods: {
        resetModal() {
            this.selectedColumnNames.length = 0;
        },
        resetColumnDialog() {
            
        },
        vaidateChanges(bvModalEvt) {
            console.log("vaidateChanges..");
            bvModalEvt.preventDefault();
            this.submitChanges();
            this.$nextTick(() => {
                this.$bvModal.hide('modal-settings-dialog')
            });
        },
        submitChanges() {
            console.log("submitChanges..");
            this.$nextTick(() => {
                this.$bvModal.hide('modal-settings-dialog')
            });
        }
    }
}
</script>