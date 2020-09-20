import Vuex from 'vuex'

// Create store
const createStore = () => {
    return new Vuex.Store({
        state: {
            headerInfo: [],
            fileName: "C:\\pivotfiles\\emp_data3.csv",
            selectedColumnList: [],
            columnNameList: [],
            primaryColumns: [],
            SummaryRows: {},
            SummaryColumns:{},
            SummaryValues: [],
            SummaryFilters: {}
        },
        actions: {
            headerInfo(context) {
                this.$axios({
                    method: "GET",
                    url: 'http://127.0.0.1:5000/workspaces',
                    headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
                }).then(response => {
                    const res = response.data;
                    if(res.status == "success") {
                        context.commit('headerInfo', res.data);
                    }
                }).catch(err => console.error(err));
            }
        },
        mutations: {
            headerInfo(state, payload) {
                state.headerInfo = payload;
            },
            fileName(state, payload) {
                state.fileName = payload;
            },
            selectedColumnList(state, payload) {
                state.selectedColumnList = payload;
            },
            columnNameList(state, payload) {
                state.columnNameList = payload;
            },
            primaryColumns(state, payload) {
                state.primaryColumns = payload;
            },
            SummaryColumns(state, payload) {
                state.SummaryColumns = payload;
            },
            SummaryValues(state, payload) {
                state.SummaryValues = payload;
            },
            SummaryRows(state, payload) {
                state.SummaryRows = payload;
            },
            SummaryFilters(state, payload) {
                state.SummaryFilters = payload;
            }
        },
        getters: {
            
        }    
    })
}

export default createStore;