var app = new Vue({
    el: '#app',
    data: {
        message: 'Hello Vue!',
        editor: null,
        standardSQLEditor: null,
        standardPyEditor: null,
        servingSQLEditor: null,
        servingPyEditor: null,
        pipelines: {},
        selectedPipeline: null,
        selectedPipelineTask: null,
        currentStage: null,
        currentTask: null,
        results: {},
        workingDir: "",
        dbxJobName: "",
        currentPipelineObject: null,
        showCodeEditor: false,
        currentPipelineStagingTask: null,
        currentPipelineStagingTaskIsRunning: false,
        currentPipelineStandardTask: null,
        currentPipelineStandardTaskIsRunning: false,
        currentPipelineServingTask: null,
        currentPipelineServingTaskIsRunning: false,
        loadSampleDataIsRunning: false,
        deployIsRunning: false,
        deployAndRunIsRunning: false,
        previewModal: null,
        previewDiagram: null,
        previewWorkflowJson: '',
        stagingOutputTableFormatChanged: false,
        stagingOutputFileFormatChanged: false,
        stagingOutputViewFormatChanged: false,
        currentPageName: 'general',

    },
    methods: {
        initStandardSQLEditor: function () {
            if (this.standardSQLEditor != null) {
                return
            }
            var that = this
            const elm = window.document.getElementById("config-ui-standard-sql")
            if (elm) {
                this.standardSQLEditor = CodeMirror(elm, {
                    lineNumbers: true,
                    autoRefresh: true,
                    mode: "sql",
                    theme: "material",
                    foldGutter: true,
                });
                this.standardSQLEditor.on("blur", function () {
                    that.currentPipelineStandardTask['code']['sql'] = that.standardSQLEditor.getValue();
                })
                this.standardSQLEditor.setSize("100%", "100%");
                this.standardSQLEditor.refresh()
            }
        },
        initServingSQLEditor: function () {
            if (this.servingSQLEditor != null) {
                return
            }
            var that = this
            const elm = window.document.getElementById("config-ui-serving-sql")
            if (elm) {
                this.servingSQLEditor = CodeMirror(elm, {
                    lineNumbers: true,
                    autoRefresh: true,
                    mode: "sql",
                    theme: "material",
                    foldGutter: true,
                });
                this.servingSQLEditor.on("blur", function () {
                    that.currentPipelineServingTask['code']['sql'] = that.servingSQLEditor.getValue();
                })
                this.servingSQLEditor.setSize("100%", "100%");
                this.servingSQLEditor.refresh()
                return true
            } else {
                return false
            }
        },
        initStandardPyEditor: function () {
            if (this.standardPyEditor != null) {
                return
            }
            var that = this
            const elm = window.document.getElementById("config-ui-standard-python")
            if (elm) {
                this.standardPyEditor = CodeMirror(elm, {
                    lineNumbers: true,
                    autoRefresh: true,
                    mode: "python",
                    theme: "material",
                    foldGutter: true,
                });
                this.standardPyEditor.on("blur", function () {
                    that.currentPipelineStandardTask['code']['python'] = that.standardPyEditor.getValue();
                })
                this.standardPyEditor.setSize("100%", "100%");
                this.standardPyEditor.refresh()
                return true
            } else {
                return false
            }
        },
        initServingPyEditor: function () {
            if (this.servingPyEditor != null) {
                return
            }
            var that = this
            const elm = window.document.getElementById("config-ui-serving-python")
            if (elm) {
                this.servingPyEditor = CodeMirror(elm, {
                    lineNumbers: true,
                    autoRefresh: true,
                    mode: "python",
                    theme: "material",
                    foldGutter: true,
                });
                this.servingPyEditor.on("blur", function () {
                    that.currentPipelineServingTask['code']['python'] = that.servingPyEditor.getValue();
                })
                this.servingPyEditor.setSize("100%", "100%");
                this.servingPyEditor.refresh()
            }
        },
        initEditor: function () {
            var that = this
            if (this.editor == null) {
                this.editor = CodeMirror(window.document.getElementById("config-json-content"), {
                    lineNumbers: true,
                    autoRefresh: true,
                    mode: "javascript",
                    theme: "material",
                    foldGutter: true,
                });
                this.editor.on("blur", function () {
                    that.pipelines[that.selectedPipeline] = JSON.parse(that.editor.getValue());
                    that.loadCurrentPipelineObject(that.pipelines[that.selectedPipeline])

                })
                this.editor.setSize("100%", "100%");
                this.editor.refresh()
            }
        },
        toggleCodeEditor() {
            this.showCodeEditor = !this.showCodeEditor
            var that = this
            setTimeout(function () {
                var json = JSON.stringify(that.currentPipelineObject, null, 2)
                that.editor.setValue(json)
                that.editor.refresh()
                that.$forceUpdate();
            }, 200)
        },
        loadCurrentPipelineObject(obj) {
            this.currentPipelineObject = obj
            this.$forceUpdate()
        },
        loadPipelineResult: function (working_dir) {
            var that = this
            var stages = ["staging", "standard", "serving"]
            stages.forEach(function (stage) {
                if (that.currentPipeline[stage]) {
                    that.results[stage] = {}
                    Object.keys(that.currentPipeline[stage]).forEach(task => {
                        that.showPipelineResult(stage, task, working_dir)
                    })
                }
            })
        },
        showPipelineResult: function (stage, task, working_dir) {

            var that = this
            that.results[stage][task] = []
            that.$forceUpdate()
            axios.post('/api/pipeline/result', {
                pipeline: this.currentPipeline,
                stage: stage,
                task: task,
                working_dir: working_dir,
                limit: 30
            })
                .then(function (response) {
                    that.results[stage][task] = response.data
                    that.$forceUpdate()

                })
                .catch(function (error) {
                    console.log(error);
                });
        },

        tryStandardTask: function (task) {
            var that = this
            that.currentPipelineStandardTaskIsRunning = true
            axios.post('/api/pipeline/standardization/try', {
                pipeline: this.currentPipelineObject,
                task: task['name'],
                limit: 20,
                timeout: 10
            })
                .then(function (response) {
                    that.results["standard"] = that.results["standard"] || {}
                    that.results["standard"][task['name']] = JSON.parse(response.data.data)
                    that.currentPipelineStandardTaskIsRunning = false
                    that.$forceUpdate()

                })
                .catch(function (error) {
                    console.log(error);
                    that.currentPipelineStandardTaskIsRunning = false
                });
        },
        tryServingTask: function (task) {
            var that = this
            that.currentPipelineServingTaskIsRunning = true
            axios.post('/api/pipeline/serving/try', {
                pipeline: this.currentPipelineObject,
                task: task['name'],
                limit: 20,
                timeout: 10
            })
                .then(function (response) {
                    that.results["serving"] = that.results["serving"] || {}
                    that.results["serving"][task['name']] = JSON.parse(response.data.data)
                    that.currentPipelineServingTaskIsRunning = false
                    that.$forceUpdate()

                })
                .catch(function (error) {
                    console.log(error);
                    that.currentPipelineServingTaskIsRunning = false
                });
        },
        deployPipeline(runNow) {
            var that = this
            if (runNow) {
                this.deployAndRunIsRunning = true
            } else {
                this.deployIsRunning = true
            }
            axios.post('/api/pipeline/deploy', {
                pipeline: this.currentPipeline,
                job_name: this.dbxJobName,
                working_dir: this.workingDir,
                row_now: runNow
            })
                .then(function (response) {
                    alert("The data pipeline is deployed successfully!")
                    that.$forceUpdate()
                    if (runNow) {
                        that.deployAndRunIsRunning = false
                    } else {
                        that.deployIsRunning = false
                    }
                })
                .catch(function (error) {
                    console.log(error);
                    if (runNow) {
                        that.deployAndRunIsRunning = false
                    } else {
                        that.deployIsRunning = false
                    }
                });
        },
        previewPipelineWorkflow() {
            var that = this
            that.previewWorkflowJson = ''
            axios.post('/api/pipeline/workflow/preview', {
                pipeline: this.currentPipeline,
                job_name: this.dbxJobName,
                working_dir: this.workingDir
            })
                .then(function (response) {
                    that.previewWorkflowJson = JSON.stringify(response.data.json, null, 4)
                    that.$forceUpdate()
                })
                .catch(function (error) {
                    console.log(error);
                });
        },
        openFile() {
            var that = this
            var func = function dispFile(contents, filename) {
                // document.getElementById('config-content').innerHTML = contents
                var pipelineObj = JSON.parse(contents);
                that.selectedPipeline = pipelineObj["name"];
                that.pipelines[that.selectedPipeline] = pipelineObj
                that.results = {}
                that.loadCurrentPipelineObject(pipelineObj)
                if (that.currentPipelineObject) {
                    if (that.currentPipelineObject['staging'] && that.currentPipelineObject['staging'].length > 0) {
                        that.currentPipelineStagingTask = that.currentPipelineObject['staging'][0]
                    } else {
                        that.currentPipelineStagingTask = null
                    }
                    if (that.currentPipelineObject['standard'] && that.currentPipelineObject['standard'].length > 0) {
                        that.currentPipelineStandardTask = that.currentPipelineObject['standard'][0]

                    } else {
                        that.currentPipelineStandardTask = null

                    }

                    if (that.currentPipelineObject['serving'] && that.currentPipelineObject['serving'].length > 0) {
                        that.currentPipelineServingTask = that.currentPipelineObject['serving'][0]
                    } else {
                        that.currentPipelineServingTask = null
                    }

                    setTimeout(function () {
                        that.onStandardTaskChanged()
                        that.onServingTaskChanged()
                    }, 1000)
                }
                that.$forceUpdate();
                setTimeout(function () {
                    that.editor.setValue(contents);
                    that.editor.refresh();
                    that.workingDir = "/FileStore/cddp_apps/"
                    that.dbxJobName = that.currentPipelineName + "_pipeline"
                    that.$forceUpdate();
                }, 200)



            }
            readFile = function (e) {
                var file = e.target.files[0];
                if (!file) {
                    return;
                }
                var reader = new FileReader();
                reader.onload = function (e) {
                    var contents = e.target.result;
                    fileInput.func(contents, file.name)
                    document.body.removeChild(fileInput)
                }
                reader.readAsText(file)
            }
            fileInput = document.createElement("input")
            fileInput.type = 'file'
            fileInput.style.display = 'none'
            fileInput.onchange = readFile
            fileInput.func = func
            document.body.appendChild(fileInput)
            that.clickElem(fileInput)
        },

        loadSampleData(task) {
            var that = this
            that.loadSampleDataIsRunning = true
            var func = function dispFile(content, filename) {
                var format = "csv"
                //check if filename is csv
                if (filename.endsWith(".csv")) {
                    format = "csv"
                } else if (filename.endsWith(".json")) {
                    format = "json"
                }
                axios.post('/api/pipeline/staging/load_sample_data', {
                    sample_data: content,
                    sample_data_format: format,
                })
                    .then(function (response) {
                        task.schema = JSON.parse(response.data.schema)
                        task.sampleData = JSON.parse(response.data.data)
                        console.log(task.schema);
                        console.log(task.sampleData)
                        that.$forceUpdate()
                        that.loadSampleDataIsRunning = false
                    })
                    .catch(function (error) {
                        console.log(error);
                        that.loadSampleDataIsRunning = false
                    });

            }
            readDataFile = function (e) {
                var file = e.target.files[0];
                if (!file) {
                    return;
                }
                var reader = new FileReader();
                reader.onload = function (e) {
                    var content = e.target.result;
                    fileInput.func(content, file.name)
                    document.body.removeChild(fileInput)
                }
                reader.readAsText(file)
            }
            fileInput = document.createElement("input")
            fileInput.type = 'file'
            fileInput.style.display = 'none'
            fileInput.onchange = readDataFile
            fileInput.func = func
            document.body.appendChild(fileInput)
            that.clickElem(fileInput)
        },
        closeFile() {
            this.pipelines[this.selectedPipeline] = null;
            delete this.pipelines[this.selectedPipeline]
            if (Object.keys(this.pipelines).length > 0) {
                this.selectedPipeline = Object.keys(this.pipelines)[0];
            } else {
                this.selectedPipeline = null;
            }
            this.onPipelineChanged()
            this.$forceUpdate()

        },
        clickElem(elem) {
            var eventMouse = window.document.createEvent("MouseEvents")
            eventMouse.initMouseEvent("click", true, false, window, 0, 0, 0, 0, 0, false, false, false, false, 0, null)
            elem.dispatchEvent(eventMouse)
        },
        onStandardCodeTypeChanged() {
            this.onStandardTaskChanged()
        },
        onStandardTaskChanged() {
            const that = this
            if (!this.currentPipelineStandardTask) {
                if (this.standardSQLEditor != null) {
                    this.standardSQLEditor.setValue("")
                    setTimeout(function () {
                        that.standardSQLEditor.refresh()
                    }, 100);
                }

                if (this.standardPyEditor != null) {
                    this.standardPyEditor.setValue("")
                    setTimeout(function () {
                        that.standardPyEditor.refresh()
                    }, 100);
                }
            }
            else if (this.currentPipelineStandardTask['code']['lang'] == 'sql') {
                this.initStandardSQLEditor()
                if(that.standardSQLEditor!=null) {
                    setTimeout(function () {
                        if (!that.currentPipelineStandardTask['code']['sql']) {
                            that.currentPipelineStandardTask['code']['sql'] = ""
                        }
                        that.standardSQLEditor.setValue(that.currentPipelineStandardTask['code']['sql'])
                        setTimeout(function () {
                            that.standardSQLEditor.refresh()
                        }, 500);
                    }, 500);
                }
            } else if (this.currentPipelineStandardTask['code']['lang'] == 'python') {
                this.initStandardPyEditor()
                if(that.standardPyEditor==null) {
                    setTimeout(function () {
                        if (!that.currentPipelineStandardTask['code']['python']) {
                            that.currentPipelineStandardTask['code']['python'] = ""
                        }
                        that.standardPyEditor.setValue(that.currentPipelineStandardTask['code']['python'])
                        setTimeout(function () {
                            that.standardPyEditor.refresh()
                        }, 500);

                    }, 500);
                }
            }
        },
        onServingCodeTypeChanged() {
            this.onServingTaskChanged()
        },
        onServingTaskChanged() {
            const that = this
            if (!this.currentPipelineServingTask) {
                if (this.servingSQLEditor != null) {
                    this.servingSQLEditor.setValue("")
                    setTimeout(function () {
                        that.servingSQLEditor.refresh()
                    }, 100);
                }

                if (this.servingPyEditor != null) {
                    this.servingPyEditor.setValue("")
                    setTimeout(function () {
                        that.servingPyEditor.refresh()
                    }, 100);
                }
            }
            else if (this.currentPipelineServingTask['code']['lang'] == 'sql') {
                this.initServingSQLEditor()
                if(that.servingSQLEditor!=null) {
                    setTimeout(function () {
                        if (!that.currentPipelineServingTask['code']['sql']) {
                            that.currentPipelineServingTask['code']['sql'] = ""
                        }
                        that.servingSQLEditor.setValue(that.currentPipelineServingTask['code']['sql'])
                        setTimeout(function () {
                            that.servingSQLEditor.refresh()
                        }, 500);
                    }, 500);
                }
            } else if (this.currentPipelineServingTask['code']['lang'] == 'python') {
                this.initServingPyEditor()
                if(that.servingPyEditor==null) {
                    setTimeout(function () {
                        if (!that.currentPipelineServingTask['code']['python']) {
                            that.currentPipelineServingTask['code']['python'] = ""
                        }
                        that.servingPyEditor.setValue(that.currentPipelineServingTask['code']['python'])
                        setTimeout(function () {
                            that.servingPyEditor.refresh()
                        }, 500);

                    }, 500);
                }
            }
        },
        onPipelineChanged() {
            var that = this
            if (this.selectedPipeline) {

                this.currentPipelineObject = this.pipelines[this.selectedPipeline]
                if (that.currentPipelineObject) {
                    if (that.currentPipelineObject['staging'] && that.currentPipelineObject['staging'].length > 0) {
                        that.currentPipelineStagingTask = that.currentPipelineObject['staging'][0]
                    } else {
                        that.currentPipelineStagingTask = null
                    }
                    if (that.currentPipelineObject['standard'] && that.currentPipelineObject['standard'].length > 0) {
                        that.currentPipelineStandardTask = that.currentPipelineObject['standard'][0]
                    } else {
                        that.currentPipelineStandardTask = null
                    }
                    if (that.currentPipelineObject['serving'] && that.currentPipelineObject['serving'].length > 0) {
                        that.currentPipelineServingTask = that.currentPipelineObject['serving'][0]
                    } else {
                        that.currentPipelineServingTask = null
                    }
                }
                this.$forceUpdate()
                var json = JSON.stringify(this.pipelines[this.selectedPipeline], null, 2)
                this.editor.setValue(json)
                this.editor.refresh()

                this.workingDir = "dbfs:/FileStore/cddp_apps/" + this.currentPipelineName + "/"
                this.dbxJobName = this.currentPipelineName + "_pipeline"

            } else {
                this.editor.setValue("");
                this.editor.refresh();
            }
        },
        onPipelineTaskChanged() {
            if (this.selectedPipelineTask) {
                this.currentStage = this.selectedPipelineTask[0]
                this.currentTask = this.selectedPipelineTask[1]
                this.showPipelineResult();
            }

        },
        showPreview() {
            var that = this
            this.previewModal.show()
            setTimeout(function () {
                that.previewDiagram = that.buildDiagram()
                that.previewPipelineWorkflow()
                that.$forceUpdate();
            }, 500)

        },
        selectPage(pageName, taskName) {
            const that = this
            this.currentPageName = pageName
            if(this.currentPageName == 'staging') {
                that.currentPipelineObject['staging'].forEach(function (task) {
                    if (task['name'] == taskName) {
                        that.currentPipelineStagingTask = task
                    }
                })
            } else if(this.currentPageName == 'standard') {
                that.currentPipelineObject['standard'].forEach(function (task) {
                    if (task['name'] == taskName) {
                        that.currentPipelineStandardTask = task
                        that.onStandardTaskChanged()
                    }
                } )
            } else if(this.currentPageName == 'serving') {
                that.currentPipelineObject['serving'].forEach(function (task) {
                    if (task['name'] == taskName) {
                        that.currentPipelineServingTask = task
                        that.onServingTaskChanged()
                    }
                })
            }
        },
        buildDiagram() {

            var md = ''
            if (this.currentPipelineObject) {
                var stagingNode = ['standard_gate{{Standardization Gate}}']
                var stagingConn = []
                this.currentPipelineObject["staging"].forEach(function (task) {
                    if (task['input']['type'] == 'filestore') {
                        var mode = task['input']['read-type']
                        if (mode == 'batch') {
                            stagingNode.push(task['name'] + "[" + task['name'] + "]")
                            stagingConn.push(task['name'] + " --> standard_gate")
                        } else {
                            stagingNode.push(task['name'] + "([" + task['name'] + "])")
                        }
                    }

                })

                var standardNode = ['serving_gate{{Serving Gate}}']
                var standardConn = []
                this.currentPipelineObject["standard"].forEach(function (task) {
                    if (task['type'] == 'batch') {
                        standardNode.push(task['name'] + "[" + task['name'] + "]")
                        standardConn.push(task['name'] + " --> serving_gate")
                    } else {
                        standardNode.push(task['name'] + "([" + task['name'] + "])")

                    }
                    standardConn.push("standard_gate --> " + task['name'])
                    if(task['dependency']) {
                        task['dependency'].forEach(function (dep) {
                            standardConn.push(dep + " --> " + task['name'])
                        })
                    }

                })

                standardConn.push("standard_gate --> serving_gate")

                var servingNode = []
                var servingConn = []
                this.currentPipelineObject["serving"].forEach(function (task) {
                    if (task['type'] == 'batch') {
                        servingNode.push(task['name'] + "[" + task['name'] + "]")
                    } else {
                        servingNode.push(task['name'] + "([" + task['name'] + "])")
                    }
                    servingConn.push("serving_gate --> " + task['name'])
                    if(task['dependency']) {
                        task['dependency'].forEach(function (dep) {
                            servingConn.push(dep + " --> " + task['name'])
                        })
                    }
                })

                var md = `graph TD
                ${stagingNode.join('\n')}                    
                ${standardNode.join('\n')}                   
                ${servingNode.join('\n')}
                ${standardConn.join('\n')}
                ${stagingConn.join('\n')}
                ${servingConn.join('\n')}`

                console.log(md);

                return md
            } else {
                return `graph TD`
            }
        },
        newPipeline() {
            var that = this
            var i = 1;
            //create untitled pipeline
            while (this.pipelines["untitled" + i]) {
                i++;
            }
            var name = "untitled_" + i
            var contents = {
                "name": name,
                "staging": [],
                "standard": [],
                "serving": []
            }


            that.pipelines[name] = contents;
            that.selectedPipeline = name;
            that.loadCurrentPipelineObject(contents)
            that.workingDir = "/FileStore/cddp_apps/" + that.currentPipelineName + "/"
            that.dbxJobName = that.currentPipelineName + "_pipeline"
            that.$forceUpdate();

        },
        newStagingTask() {
            var that = this
            var i = 1;
            //create untitled task
            while (this.currentPipelineObject["staging"].find(function (task) {
                return task.name == "stg_untitled" + i
            })) {
                i++;
            }
            var name = "stg_untitled" + i
            var newTask = {
                "name": name,
                "target": name,
                "input": {
                    "type": "filestore",
                    "format": "csv",
                    "path": name + "/",
                    "read-type": "batch",
                    "schema": {}
                },
                "output": {
                    "target": name,
                    "type": [
                        "file",
                        "view"
                    ]
                },
                schema: {},
                sampleData: []

            }

            that.currentPipelineObject['staging'].push(newTask)
            that.currentPipelineStagingTask = that.currentPipelineObject['staging'][that.currentPipelineObject['staging'].length - 1]
        },
        newStandardTask() {
            var that = this
            var i = 1
            while (this.currentPipelineObject["standard"].find(function (task) {
                return task.name == "std_untitled" + i
            })) {
                i++;
            }
            var name = "std_untitled" + i
            var newTask = {
                "name": name,
                "code": {
                    "lang": "sql",
                    "sql": "select now() as ts",
                    "python": "output_df = spark.sql('select now() as ts')",
                },
                "type": "batch",
                "output": {
                    "target": name,
                    "type": [
                        "file",
                        "view"
                    ]
                },
                "dependency":[]
            }
            that.currentPipelineObject['standard'].push(newTask)
            that.currentPipelineStandardTask = that.currentPipelineObject['standard'][that.currentPipelineObject['standard'].length - 1]
        },
        newServingTask() {
            var that = this
            var i = 1
            while (this.currentPipelineObject["serving"].find(function (task) {
                return task.name == "srv_untitled" + i
            })) {
                i++;
            }
            var name = "srv_untitled" + i
            var newTask = {
                "name": name,
                "code": {
                    "lang": "sql",
                    "sql": "select now() as ts",
                    "python": "output_df = spark.sql('select now() as ts')",
                },
                "type": "batch",
                "output": {
                    "target": name,
                    "type": [
                        "file",
                        "view"
                    ]
                },
                "dependency":[]
            }
            that.currentPipelineObject['serving'].push(newTask)
            that.currentPipelineServingTask = that.currentPipelineObject['serving'][that.currentPipelineObject['serving'].length - 1]
        }
    },

    computed: {
        currentPipeline() {
            if (this.currentPipelineObject) {
                return this.currentPipelineObject
            } else {
                return null
            }
        },
        currentPipelineStagingTasks() {
            if (this.currentPipeline["staging"]) {
                return Object.keys(this.currentPipeline["staging"])
            } else {
                return []
            }

        },
        currentPipelineStardardTasks() {
            if (this.currentPipeline["standard"]) {
                return Object.keys(this.currentPipeline["standard"])
            } else {
                return []
            }
        },
        currentPipelineServingTasks() {
            if (this.currentPipeline["serving"]) {
                return Object.keys(this.currentPipeline["serving"])
            } else {
                return []
            }
        },
        currentPipelineName() {
            if (this.selectedPipeline) {
                return this.currentPipeline.name
            } else {
                return null;
            }
        },

    },
    mounted: function () {
        this.initEditor();
        this.previewModal = new bootstrap.Modal('#previewModal', {
            keyboard: false
        })
        var that = this
    }
})