<template>
    <section class="w-100">
        <b-row>
            <b-col md="2">
                <label>Choose Fields: </label>
                <draggable class="dragArea list-group" :list="columnNames"
                    :group="{ name: 'fieldNames', pull: 'clone', put: false }"
                    :clone="cloneField">
                    <!-- <b-form-group label="Choose fields">
                        <b-form-checkbox-group
                            v-model="selectedField"
                            :options="fieldsArray"
                            stacked
                        ></b-form-checkbox-group>
                    </b-form-group> -->
                    <div class="list-group-item" v-for="(value, idx)  in columnNames" :key="idx">
                        {{ value }}
                    </div>
                </draggable>
            </b-col>

            <b-col md="10">
                    <b-row>
                        <b-col>
                            <draggable class="dragArea list-group" :list="columnArray" 
                            @change="columnChange" group="fieldNames">
                                <b-form-group label="COLUMNS" label-for="input_columns">
                                    <b-form-select id="input_columns" :options="columnArray"></b-form-select>
                                </b-form-group>
                                <!-- <div class="list-group-item" v-for="(field, idx) in columnArray" :key="idx">
                                    {{ field }}
                                </div> -->
                            </draggable>
                        </b-col>

                        <b-col>                
                            <draggable class="dragArea list-group" :list="filterArray" 
                            @change="filterChange" group="fieldNames">
                                <b-form-group label="FILTERS" label-for="input_filters">
                                    <b-form-select id="input_filters" :options="filterArray"></b-form-select>
                                </b-form-group>
                                <!-- <div class="list-group-item" v-for="(column, idx) in filterArray" :key="idx">
                                    {{ column }}
                                </div> -->
                            </draggable>
                        </b-col>

                        <b-col>
                            <draggable class="dragArea list-group" :list="valueArray" 
                            @change="log" group="fieldNames">
                                <b-form-group label="VALUES" label-for="input_values">
                                    <b-form-select id="input_values" :options="valueArray"></b-form-select>
                                </b-form-group>                    
                                <!-- <div class="list-group-item" v-for="(column, idx) in valueArray" :key="idx">
                                    {{ column }}
                                </div> -->
                            </draggable>
                        </b-col>

                        <b-col>
                            <draggable class="dragArea list-group" :list="rowArray" 
                            @change="log" group="fieldNames">
                                <b-form-group label="ROWS" label-for="input_rows">
                                    <b-form-select id="input_rows" :options="rowArray"></b-form-select>
                                </b-form-group>
                                <!-- <div class="list-group-item" v-for="(column, idx) in rowArray" :key="idx">
                                    {{ column }}
                                </div> -->
                            </draggable>
                        </b-col>

                        <b-col>
                            <b-button class="mt-4">Submit</b-button>
                        </b-col>
                    </b-row>

                    <!-- <AlertMessage :alertMsg="alertMsg" /> -->
                    <div class="border border-dark p-4 d-flex justify-content-center align-items-center">
                        
                        <span v-if="finalResults.length === 0">results will show here....</span>
                        <div class="w-100" v-else> 
                            <b-table striped hover :items="finalResults" head-variant="light"
                                sticky-header></b-table>
                            <b-button :disabled="offset <= 1 || isBusy" @click="onPrev">Prev</b-button>
                            <b-button :disabled="totalRows < (this.finalResults.length + 1) || isBusy" @click="onNext">Next</b-button>
                        </div>
                    </div>
                <!-- <b-button v-b-modal.modal-settings-dialog>Validate</b-button> -->
            </b-col>
        </b-row>

        <!-- modal dialog -->
        <Modal />
    </section>
</template>

<script>
import draggable from "vuedraggable"
// import AlertMessage from "~/components/AlertMessage"
import Modal from "~/components/Modal"
export default {
    props: {
        columnNames: Array,
        columnDatatypes: Array,
        finalResults: Array
    },
    components: {
        draggable,
        Modal
    },
    data: () => {
        return {
            columnArray: [],
            filterArray: [],
            valueArray: [],
            rowArray: [],
            isBusy: false,
            alertMsg: {
                text: "",
                status: false
            }
        }
    },
    watch: {
        columnNames(val) {
            this.$store.commit("columnNameList", val);
        }
    },
    methods: {
        log: function(evt) {
            console.log("log: ", evt);
            // this.$bvModal.show('modal-settings-dialog');
        },
        columnChange() {
            this.$bvModal.show('column-settings-dialog');
        },
        filterChange() {
            this.$bvModal.show('filter-settings-dialog');
        },
        cloneField(name) {
            console.log("cloneField: ", name);            
            return `${name}`;
        },
        
    }
}
</script>