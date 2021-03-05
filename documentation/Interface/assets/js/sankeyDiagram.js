var sankeyChart = echarts.init(document.getElementById('sankeyDiagram'));

sankey = {
    "top": "5%",
    "bottom": "15%",
    "left": "1%",
    "right": "1%",
    "type": "sankey",
    "name": "Sankey",
    "focusNodeAdjacency": "allEdges",
    "nodeAlign": "center",
    "layout": "none",
    "data": [
        {
            "": "0",
            "user_ids": [
                114932,
                432898,
                452425,
                465308,
                480726,
                574109,
                575523,
                585456,
                599789,
                717621,
                866133
            ],
            "support": "25",
            "itemsets": "4915",
            "period": "2017-07-01",
            "property_values": "1",
            "depth": 0,
            "size": 14,
            "sex": "1",
            "itemset_name": "'[\"SERVIT.MARSUPILAMI PAP ETE 2017\"]'",
            "name": "0",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "1",
            "user_ids": [
                106174,
                412666,
                427150,
                460243,
                465004,
                465308,
                476566,
                530327,
                532504,
                563195,
                625091,
                835363,
                863705,
                874591,
                957568
            ],
            "support": "29",
            "itemsets": "4112",
            "period": "2017-11-01",
            "property_values": "1",
            "depth": 5882.35294117647,
            "size": 18,
            "sex": "1",
            "itemset_name": "'[\"LAVAGE CLU\"]'",
            "name": "1",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "2",
            "user_ids": [
                106174,
                264679,
                274132,
                321135,
                332635,
                365367,
                438454,
                465004,
                499127,
                503579,
                532504,
                565442,
                574109,
                615324,
                957568,
                964386
            ],
            "support": "30",
            "itemsets": "5631",
            "period": "2017-11-01",
            "property_values": "1",
            "depth": 5882.35294117647,
            "size": 19,
            "sex": "1",
            "itemset_name": "'[\"TWIX 50G\"]'",
            "name": "2",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "4",
            "user_ids": [
                107216,
                835363,
                835363,
                835363,
                835363,
                837950,
                874591,
                923982,
                957568
            ],
            "support": "20",
            "itemsets": "4112",
            "period": "2017-12-01",
            "property_values": "1",
            "depth": 11764.70588235294,
            "size": 12,
            "sex": "1",
            "itemset_name": "'[\"LAVAGE CLU\"]'",
            "name": "4",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "5",
            "user_ids": [
                585456,
                598886,
                615324,
                711978,
                820248,
                824262,
                866133,
                900950,
                923982
            ],
            "support": "34",
            "itemsets": "4112",
            "period": "2018-01-01",
            "property_values": "1",
            "depth": 17647.058823529413,
            "size": 12,
            "sex": "1",
            "itemset_name": "'[\"LAVAGE CLU\"]'",
            "name": "5",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "7",
            "user_ids": [
                103240,
                384542,
                390199,
                423878,
                438114,
                443706,
                519327,
                574109,
                578902,
                621094,
                717621,
                749466,
                805158,
                863705,
                867805,
                874591,
                911734,
                915090,
                976245
            ],
            "support": "33",
            "itemsets": "5951",
            "period": "2018-05-01",
            "property_values": "1",
            "depth": 29411.764705882353,
            "size": 22,
            "sex": "1",
            "itemset_name": "'[\"FUZE TEA PECHE 40CL\"]'",
            "name": "7",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "9",
            "user_ids": [
                114932,
                114932,
                343142,
                386709,
                390199,
                397953
            ],
            "support": "34",
            "itemsets": "4919",
            "period": "2018-07-01",
            "property_values": "1",
            "depth": 35294.117647058825,
            "size": 9,
            "sex": "1",
            "itemset_name": "'[\"SERVIET.ASTERIX\"]'",
            "name": "9",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "11",
            "user_ids": [
                950963,
                957568,
                967128
            ],
            "support": "33",
            "itemsets": "4920",
            "period": "2018-07-01",
            "property_values": "1",
            "depth": 35294.117647058825,
            "size": 6,
            "sex": "1",
            "itemset_name": "'[\"SERVIET.OBELIX\"]'",
            "name": "11",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "12",
            "user_ids": [
                1030140
            ],
            "support": "24",
            "itemsets": "4921",
            "period": "2018-07-01",
            "property_values": "1",
            "depth": 35294.117647058825,
            "size": 4,
            "sex": "1",
            "itemset_name": "'[\"SERVIET.FALBALA\"]'",
            "name": "12",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "13",
            "user_ids": [
                116517,
                763292,
                766708,
                913899,
                923982,
                967128
            ],
            "support": "20",
            "itemsets": "4125",
            "period": "2018-07-01",
            "property_values": "1",
            "depth": 35294.117647058825,
            "size": 9,
            "sex": "1",
            "itemset_name": "'[\"RECHARGE LAVAGE TW - A\"]'",
            "name": "13",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "14",
            "user_ids": [
                115334,
                476566,
                598886,
                624174,
                711978,
                811951,
                866133
            ],
            "support": "24",
            "itemsets": "4112",
            "period": "2018-09-01",
            "property_values": "1",
            "depth": 41176.470588235294,
            "size": 11,
            "sex": "1",
            "itemset_name": "'[\"LAVAGE CLU\"]'",
            "name": "14",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
        {
            "": "16",
            "user_ids": [
                106174,
                287754,
                312415,
                317586,
                332635,
                355661,
                357876,
                373929,
                380236,
                393439,
                406056,
                423878,
                437974,
                476566,
                482512,
                483211,
                499127,
                503579,
                519327,
                524668,
                537723,
                575523,
                623370,
                711978,
                844897,
                866133,
                867805,
                965178,
                997163,
                1030140,
                1085639,
                1089136,
                1109294,
                1207494,
                1215832,
                1216244
            ],
            "support": "50",
            "itemsets": "4112",
            "period": "2019-01-01",
            "property_values": "1",
            "depth": 52941.17647058824,
            "size": 39,
            "sex": "1",
            "itemset_name": "'[\"LAVAGE CLU\"]'",
            "name": "16",
            "label": {
                "normal": {
                    "distance": -1
                }
            },
            "itemStyle": {
                "z": 1,
                "color": "rgb(27,120,0)"
            }
        },
    ],
    "links": [
        {
            "source": "0",
            "target": "1",
            "user_id": [
                390199,
                438114,
                578902
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "0",
            "target": "2",
            "user_id": [
                114932,
                132519,
                332635,
                574109
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "3",
            "target": "5",
            "user_id": [
                120286,
                390199
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "4",
            "target": "5",
            "user_id": [
                274953,
                598886,
                923982,
                274953,
                598886,
                923982,
                390199,
                438114,
                578902,
                274953,
                598886,
                923982,
                274953,
                598886,
                923982
            ],
            "value": 12,
            "label": "ST",
            "lineStyle": {
                "color": "#457b9d",
                "opacity": 1
            }
        },
        {
            "source": "2",
            "target": "6",
            "user_id": [
                332635
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "4",
            "target": "6",
            "user_id": [
                957568
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "5",
            "target": "6",
            "user_id": [
                274953,
                866133
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "6",
            "target": "9",
            "user_id": [
                336247
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "7",
            "target": "9",
            "user_id": [
                114932,
                390199,
                519327,
                574109,
                578902,
                867805
            ],
            "value": 3,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "8",
            "target": "9",
            "user_id": [
                180230
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "1",
            "target": "17",
            "user_id": [
                625091
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "13",
            "target": "17",
            "user_id": [
                149886
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "16",
            "target": "17",
            "user_id": [
                120286,
                132519,
                188588,
                437974,
                482512
            ],
            "value": 2,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "16",
            "target": "18",
            "user_id": [
                103240,
                384542,
                390199,
                423878,
                438114,
                443706,
                519327,
                574109,
                578902,
                482512
            ],
            "value": 7,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "16",
            "target": "19",
            "user_id": [
                103240,
                384542,
                390199,
                423878,
                438114,
                443706,
                519327,
                574109,
                578902,
                188588,
                437974,
                106174,
                287754,
                312415,
                317586,
                332635,
                355661,
                482512
            ],
            "value": 15,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "1",
            "target": "4",
            "user_id": [
                273864,
                274953,
                280880,
                835363,
                874591,
                957568
            ],
            "value": 3,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "2",
            "target": "4",
            "user_id": [
                957568
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "3",
            "target": "4",
            "user_id": [
                147391,
                273864,
                280880,
                598886,
                835363,
                874591
            ],
            "value": 3,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "1",
            "target": "8",
            "user_id": [
                132519
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "2",
            "target": "8",
            "user_id": [
                132519
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "3",
            "target": "8",
            "user_id": [
                355812
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "5",
            "target": "8",
            "user_id": [
                438114,
                711978
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "6",
            "target": "8",
            "user_id": [
                717621
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "7",
            "target": "11",
            "user_id": [
                193191,
                285365,
                874591
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "7",
            "target": "12",
            "user_id": [
                805158
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "3",
            "target": "16",
            "user_id": [
                221900
            ],
            "value": 1,
            "label": "G",
            "lineStyle": {
                "color": "#e63946",
                "opacity": 1
            }
        },
        {
            "source": "5",
            "target": "16",
            "user_id": [
                120286
            ],
            "value": 1,
            "label": "G",
            "lineStyle": {
                "color": "#e63946",
                "opacity": 1
            }
        },
        {
            "source": "14",
            "target": "16",
            "user_id": [
                274953,
                598886,
                923982,
                390199,
                438114,
                567,
                390199,
                438114,

            ],
            "value": 6,
            "label": "G",
            "lineStyle": {
                "color": "#e63946",
                "opacity": 1
            }
        },
        {
            "source": "9",
            "target": "16",
            "user_id": [
                114932,
                499127,
                114932,
                499127,
                519327,
                867805
            ],
            "value": 3,
            "label": "G",
            "lineStyle": {
                "color": "#e63946",
                "opacity": 1
            }
        },
        {
            "source": "11",
            "target": "14",
            "user_id": [
                193191,
                285365,
                874591,
                678,
                678854,
                34575,
                45678,
                5775,
                5664
            ],
            "value": 6,
            "label": "M",
            "lineStyle": {
                "color": "#a8dadc",
                "opacity": 1
            }
        },
        {
            "source": "12",
            "target": "14",
            "user_id": [
                805158,
                5678,
                567,
                678,
                678,
                677588,
                6789
            ],
            "value": 4,
            "label": "M",
            "lineStyle": {
                "color": "#a8dadc",
                "opacity": 1
            }
        },
        {
            "source": "15",
            "target": "16",
            "user_id": [
                115022,
                259132,
                482512
            ],
            "value": 1,
            "label": "G",
            "lineStyle": {
                "color": "#e63946",
                "opacity": 1
            }
        },
        {
            "source": "2",
            "target": "7",
            "user_id": [
                114932,
                574109
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "5",
            "target": "7",
            "user_id": [
                390199,
                438114,
                578902
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "6",
            "target": "7",
            "user_id": [
                717621
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "2",
            "target": "13",
            "user_id": [
                226924
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "3",
            "target": "13",
            "user_id": [
                176750
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "5",
            "target": "13",
            "user_id": [
                923982
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "7",
            "target": "13",
            "user_id": [
                149886
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "3",
            "target": "10",
            "user_id": [
                285090
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "5",
            "target": "10",
            "user_id": [
                820248
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "7",
            "target": "10",
            "user_id": [
                717621
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "8",
            "target": "10",
            "user_id": [
                717621
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "6",
            "target": "14",
            "user_id": [
                274953,
                866133
            ],
            "value": 1,
            "label": "M",
            "lineStyle": {
                "color": "#a8dadc",
                "opacity": 1
            }
        },
        {
            "source": "13",
            "target": "14",
            "user_id": [
                176750,
                343202
            ],
            "value": 1,
            "label": "M",
            "lineStyle": {
                "color": "#a8dadc",
                "opacity": 1
            }
        },
        {
            "source": "5",
            "target": "15",
            "user_id": [
                262442
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "6",
            "target": "15",
            "user_id": [
                115022
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "9",
            "target": "15",
            "user_id": [
                574109
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "10",
            "target": "15",
            "user_id": [
                820248
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "14",
            "target": "15",
            "user_id": [
                176750,
                274953,
                343202
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "5",
            "target": "18",
            "user_id": [
                530327
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "13",
            "target": "18",
            "user_id": [
                226924
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "14",
            "target": "16",
            "user_id": [
                231893,
                280880,
                438114
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "16",
            "target": "18",
            "user_id": [
                259132
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "17",
            "target": "18",
            "user_id": [
                132519,
                179416,
                209276,
                231629,
                432898,
                482512,
                625091
            ],
            "value": 4,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "8",
            "target": "25",
            "user_id": [
                355812
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "25",
            "user_id": [
                530327
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "19",
            "target": "25",
            "user_id": [
                1030140
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "16",
            "target": "19",
            "user_id": [
                221900,
                711978,
                1030140
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "19",
            "user_id": [
                231629,
                432898,
                625091
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "22",
            "user_id": [
                642259
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "19",
            "target": "22",
            "user_id": [
                336247,
                805158
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "20",
            "target": "22",
            "user_id": [
                345624,
                625091,
                923982
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "23",
            "user_id": [
                179416,
                530327
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "19",
            "target": "23",
            "user_id": [
                221900,
                437974,
                1030140
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "20",
            "target": "23",
            "user_id": [
                147391,
                285090,
                345624,
                711978
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "24",
            "user_id": [
                226924
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "2121",
            "target": "24",
            "user_id": [
                176750
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "26",
            "user_id": [
                226924,
                530327
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "19",
            "target": "20",
            "user_id": [
                157246,
                188588,
                285090,
                625091,
                711978
            ],
            "value": 2,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "15",
            "target": "21",
            "user_id": [
                343202,
                820248
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "21",
            "user_id": [
                176750,
                209276
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "19",
            "target": "21",
            "user_id": [
                273864,
                274953
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "20",
            "target": "21",
            "user_id": [
                133917,
                157246,
                188588,
                259132,
                280880,
                309886
            ],
            "value": 3,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "19",
            "target": "27",
            "user_id": [
                432898
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "22",
            "target": "27",
            "user_id": [
                301967
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "23",
            "target": "27",
            "user_id": [
                285090
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "28",
            "user_id": [
                438114
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "21",
            "target": "28",
            "user_id": [
                209276,
                280880,
                309886,
                209276,
                280880,
                309886
            ],
            "value": 3,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "18",
            "target": "29",
            "user_id": [
                262442
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "22",
            "target": "29",
            "user_id": [
                180230,
                274953,
                355661,
                642259
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "23",
            "target": "29",
            "user_id": [
                188588,
                274953
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "24",
            "target": "29",
            "user_id": [
                251114
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "25",
            "target": "29",
            "user_id": [
                133917,
                188588,
                285365
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "26",
            "target": "29",
            "user_id": [
                274953,
                285365
            ],
            "value": 1,
            "label": "S",
            "lineStyle": {
                "color": "#1d3557",
                "opacity": 1
            }
        },
        {
            "source": "17",
            "target": "19",
            "user_id": [
                157246,
                157246,
                157246,
                188588,
                273864,
                285090,
                437974
            ],
            "value": 4,
            "label": "M",
            "lineStyle": {
                "color": "#a8dadc",
                "opacity": 1
            }
        }
    ],
    'label': {
        distance: 0,
        position: "inside",
        formatter: 'G{b}',
    },
    "tooltip": {
        "data": [
            {
                "source": "0",
                "target": "1",
                "user_id": [
                    390199,
                    438114,
                    578902
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "0",
                "target": "2",
                "user_id": [
                    114932,
                    132519,
                    332635,
                    574109
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "1",
                "target": "5",
                "user_id": [
                    262442,
                    530327
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "3",
                "target": "5",
                "user_id": [
                    120286,
                    390199
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "4",
                "target": "5",
                "user_id": [
                    274953,
                    598886,
                    923982,
                    274953,
                    598886,
                    923982,
                    390199,
                    438114,
                    578902,
                    274953,
                    598886,
                    923982,
                    274953,
                    598886,
                    923982
                ],
                "value": 12,
                "label": "ST",
                "lineStyle": {
                    "color": "#457b9d",
                    "opacity": 1
                }
            },
            {
                "source": "2",
                "target": "6",
                "user_id": [
                    332635
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "4",
                "target": "6",
                "user_id": [
                    957568
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "5",
                "target": "6",
                "user_id": [
                    274953,
                    866133
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "6",
                "target": "9",
                "user_id": [
                    336247
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "7",
                "target": "9",
                "user_id": [
                    114932,
                    390199,
                    519327,
                    574109,
                    578902,
                    867805
                ],
                "value": 3,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "8",
                "target": "9",
                "user_id": [
                    180230
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "1",
                "target": "17",
                "user_id": [
                    625091
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "13",
                "target": "17",
                "user_id": [
                    149886
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "16",
                "target": "17",
                "user_id": [
                    120286,
                    132519,
                    188588,
                    437974,
                    482512
                ],
                "value": 2,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "16",
                "target": "18",
                "user_id": [
                    103240,
                    384542,
                    390199,
                    423878,
                    438114,
                    443706,
                    519327,
                    574109,
                    578902,
                    482512
                ],
                "value": 7,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "16",
                "target": "19",
                "user_id": [
                    103240,
                    384542,
                    390199,
                    423878,
                    438114,
                    443706,
                    519327,
                    574109,
                    578902,
                    188588,
                    437974,
                    106174,
                    287754,
                    312415,
                    317586,
                    332635,
                    355661,
                    482512
                ],
                "value": 15,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "1",
                "target": "4",
                "user_id": [
                    273864,
                    274953,
                    280880,
                    835363,
                    874591,
                    957568
                ],
                "value": 3,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "2",
                "target": "4",
                "user_id": [
                    957568
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "3",
                "target": "4",
                "user_id": [
                    147391,
                    273864,
                    280880,
                    598886,
                    835363,
                    874591
                ],
                "value": 3,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "1",
                "target": "8",
                "user_id": [
                    132519
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "2",
                "target": "8",
                "user_id": [
                    132519
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "3",
                "target": "8",
                "user_id": [
                    355812
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "5",
                "target": "8",
                "user_id": [
                    438114,
                    711978
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "6",
                "target": "8",
                "user_id": [
                    717621
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "7",
                "target": "11",
                "user_id": [
                    193191,
                    285365,
                    874591
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "7",
                "target": "12",
                "user_id": [
                    805158
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "3",
                "target": "16",
                "user_id": [
                    221900
                ],
                "value": 1,
                "label": "G",
                "lineStyle": {
                    "color": "#e63946",
                    "opacity": 1
                }
            },
            {
                "source": "5",
                "target": "16",
                "user_id": [
                    120286
                ],
                "value": 1,
                "label": "G",
                "lineStyle": {
                    "color": "#e63946",
                    "opacity": 1
                }
            },
            {
                "source": "14",
                "target": "16",
                "user_id": [
                    274953,
                    598886,
                    923982,
                    390199,
                    438114,
                    567,
                    390199,
                    438114,
                    598886,
                    923982,
                    390199,
                    438114,
                    578902,
                    923982,
                    390199,
                    438114
                ],
                "value": 13,
                "label": "G",
                "lineStyle": {
                    "color": "#e63946",
                    "opacity": 1
                }
            },
            {
                "source": "9",
                "target": "16",
                "user_id": [
                    114932,
                    499127,
                    114932,
                    499127,
                    519327,
                    867805
                ],
                "value": 3,
                "label": "G",
                "lineStyle": {
                    "color": "#e63946",
                    "opacity": 1
                }
            },
            {
                "source": "11",
                "target": "14",
                "user_id": [
                    193191,
                    285365,
                    874591,
                    678,
                    678854,
                    34575,
                    45678,
                    5775,
                    5664
                ],
                "value": 6,
                "label": "M",
                "lineStyle": {
                    "color": "#a8dadc",
                    "opacity": 1
                }
            },
            {
                "source": "12",
                "target": "14",
                "user_id": [
                    805158,
                    5678,
                    567,
                    678,
                    678,
                    677588,
                    6789
                ],
                "value": 4,
                "label": "M",
                "lineStyle": {
                    "color": "#a8dadc",
                    "opacity": 1
                }
            },
            {
                "source": "15",
                "target": "16",
                "user_id": [
                    115022,
                    259132,
                    482512
                ],
                "value": 1,
                "label": "G",
                "lineStyle": {
                    "color": "#e63946",
                    "opacity": 1
                }
            },
            {
                "source": "2",
                "target": "7",
                "user_id": [
                    114932,
                    574109
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "5",
                "target": "7",
                "user_id": [
                    390199,
                    438114,
                    578902
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "6",
                "target": "7",
                "user_id": [
                    717621
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "2",
                "target": "13",
                "user_id": [
                    226924
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "3",
                "target": "13",
                "user_id": [
                    176750
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "5",
                "target": "13",
                "user_id": [
                    923982
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "7",
                "target": "13",
                "user_id": [
                    149886
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "3",
                "target": "10",
                "user_id": [
                    285090
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "5",
                "target": "10",
                "user_id": [
                    820248
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "7",
                "target": "10",
                "user_id": [
                    717621
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "8",
                "target": "10",
                "user_id": [
                    717621
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "6",
                "target": "14",
                "user_id": [
                    274953,
                    866133
                ],
                "value": 1,
                "label": "M",
                "lineStyle": {
                    "color": "#a8dadc",
                    "opacity": 1
                }
            },
            {
                "source": "13",
                "target": "14",
                "user_id": [
                    176750,
                    343202
                ],
                "value": 1,
                "label": "M",
                "lineStyle": {
                    "color": "#a8dadc",
                    "opacity": 1
                }
            },
            {
                "source": "5",
                "target": "15",
                "user_id": [
                    262442
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "6",
                "target": "15",
                "user_id": [
                    115022
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "9",
                "target": "15",
                "user_id": [
                    574109
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "10",
                "target": "15",
                "user_id": [
                    820248
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "14",
                "target": "15",
                "user_id": [
                    176750,
                    274953,
                    343202
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "5",
                "target": "18",
                "user_id": [
                    530327
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "13",
                "target": "18",
                "user_id": [
                    226924
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "14",
                "target": "16",
                "user_id": [
                    231893,
                    280880,
                    438114
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "16",
                "target": "18",
                "user_id": [
                    259132
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "17",
                "target": "18",
                "user_id": [
                    132519,
                    179416,
                    209276,
                    231629,
                    432898,
                    482512,
                    625091
                ],
                "value": 4,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "8",
                "target": "25",
                "user_id": [
                    355812
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "25",
                "user_id": [
                    530327
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "19",
                "target": "25",
                "user_id": [
                    1030140
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "16",
                "target": "19",
                "user_id": [
                    221900,
                    711978,
                    1030140
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "19",
                "user_id": [
                    231629,
                    432898,
                    625091
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "22",
                "user_id": [
                    642259
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "19",
                "target": "22",
                "user_id": [
                    336247,
                    805158
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "20",
                "target": "22",
                "user_id": [
                    345624,
                    625091,
                    923982
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "23",
                "user_id": [
                    179416,
                    530327
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "19",
                "target": "23",
                "user_id": [
                    221900,
                    437974,
                    1030140
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "20",
                "target": "23",
                "user_id": [
                    147391,
                    285090,
                    345624,
                    711978
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "24",
                "user_id": [
                    226924
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "2121",
                "target": "24",
                "user_id": [
                    176750
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "26",
                "user_id": [
                    226924,
                    530327
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "19",
                "target": "20",
                "user_id": [
                    157246,
                    188588,
                    285090,
                    625091,
                    711978
                ],
                "value": 2,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "15",
                "target": "21",
                "user_id": [
                    343202,
                    820248
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "21",
                "user_id": [
                    176750,
                    209276
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "19",
                "target": "21",
                "user_id": [
                    273864,
                    274953
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "20",
                "target": "21",
                "user_id": [
                    133917,
                    157246,
                    188588,
                    259132,
                    280880,
                    309886
                ],
                "value": 3,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "19",
                "target": "27",
                "user_id": [
                    432898
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "22",
                "target": "27",
                "user_id": [
                    301967
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "23",
                "target": "27",
                "user_id": [
                    285090
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "28",
                "user_id": [
                    438114
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "21",
                "target": "28",
                "user_id": [
                    209276,
                    280880,
                    309886,
                    209276,
                    280880,
                    309886
                ],
                "value": 3,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "18",
                "target": "29",
                "user_id": [
                    262442
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "22",
                "target": "29",
                "user_id": [
                    180230,
                    274953,
                    355661,
                    642259
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "23",
                "target": "29",
                "user_id": [
                    188588,
                    274953
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "24",
                "target": "29",
                "user_id": [
                    251114
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "25",
                "target": "29",
                "user_id": [
                    133917,
                    188588,
                    285365
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "26",
                "target": "29",
                "user_id": [
                    274953,
                    285365
                ],
                "value": 1,
                "label": "S",
                "lineStyle": {
                    "color": "#1d3557",
                    "opacity": 1
                }
            },
            {
                "source": "17",
                "target": "19",
                "user_id": [
                    157246,
                    157246,
                    157246,
                    188588,
                    273864,
                    285090,
                    437974
                ],
                "value": 4,
                "label": "M",
                "lineStyle": {
                    "color": "#a8dadc",
                    "opacity": 1
                }
            }
        ]
    }
}


var data1 = [];
var data2 = [];
var data3 = [];

var random = function (max) {
    return (Math.random() * max).toFixed(3);
};

for (var i = 0; i < 500; i++) {
    data1.push([random(15), random(10), random(1)]);
    data2.push([random(10), random(10), random(1)]);
    data3.push([random(15), random(10), random(1)]);
}

// specify chart configuration item and data


var option = {
    grid: {
        left: '3%',
        //    right:'3%',
        top: '30%',
        //    bottom:'15%',
    },
    legend: {
        opacity: 0.6,
        textStyle: {
            fontSize: 20,
        },
        data: [
            {
                "name": "Stable",
                "lineStyle": {
                    "color": "#457b9d",
                    "opacity": 1
                }
            },

            {
                "name": "Merges",
                "lineStyle": {
                    "color": "#457b9d",
                    "opacity": 1
                }
            },
            {"name": "Splits"},
            {
                "name": "Grows",
                itemStyle: {
                    color: "#a8dadc",
                    "opacity": 1

                }
            },
            {"name": "Perishes"},
        ]
    },
    xAxis: [
        {
            position: "bottom",
            type: 'category',
            boundaryGap: false,
            nameRotate: 1,
            splitLine: {
                show: false
            },
            data: ['Jan-2017', 'Jan-2017', 'Jan-2017', 'Feb-2017', 'Mar-2017', 'Apr-2017', 'May-2017', 'Jun-2017', 'Jul-2017', 'Aug-2017', 'Sep-2017', 'Oct-2017', 'Nov-2017', 'Dec-2017', 'Feb-2018', 'Mar-2018', 'Apr-2018', 'Mai-2018', 'Jun-2018', 'Jun-2018']
        },
        {
            position: "bottom",
            offset: 25,
            zlevel: 1,
            type: 'category',
            boundaryGap: false,
            nameRotate: 2,
            splitLine: {
                show: false
            },
            axisLine: {
                show: false
            },
            axisTick: {
                show: false
            },
            data: ['Dec-2016', 'Oct-2018']
        }
    ],
    yAxis: {
        show: false,
    },
    dataZoom: [
        {
            type: 'slider',
            show: true,
            filterMode: 'weakFilter',
            start: 10,
            end: 95,

        },

    ],
    tooltip: {
        enabled: false,
        align: "bottom",
        itemGap: 1,
        padding: [0, 0, 0, 0],
        formatter: function (x) {
            return null

            if (x.dataType == "node") {
                return "<b>Group id: </b>" + x.data.name +
                    "<br><b>Size: </b>" + x.data.size +
                    "<hr><b>Users: </b>" + x.data.user_ids +
                    "<br><b>Itemset :</b>" + x.data.itemset_name +
                    "<br><b>Period :</b>" + x.data.period
            }
            if (x.dataType == "edge") {
                return "<b>Users: </b>" + x.data.user_id +
                    "<br><b>Count: </b>" + x.data.value +
                    "<br><b>Label: </b>" + x.data.label +
                    "<hr><b>" + x.data.source + "</b>" + "=><b>" + x.data.target
            } else
                return '<b>User_id :</b>' + x.data.user_id + " <br><b>Count :</b>" + x.data.value
        }
    },

    series: [
        {
            name: "Merges",
            type: 'bar',
            itemStyle: {
                color: "#e63946",
            }

        },
        {
            name: "Splits",
            type: 'bar',
            itemStyle: {
                color: "#1d3557",
            }

        },
        {
            name: "Grows",
            type: 'bar',
            itemStyle: {
                color: "#a8dadc",
                "opacity": 1

            }


        },
        {
            name: "Stable",
            type: 'bar',
            itemStyle: {
                color: "#457b9d",
            }

        },
        {
            name: "Perishes",
            type: 'bar',
            itemStyle: {
                color: "#aFFFF",
            }

        },
        {
            "top": "0%",
            "bottom": "-10%",
            "left": "0%",
            "right": "0%",
            type: 'scatter',
        },
        sankey
    ],

};
console.log(sankey)
sankeyChart.setOption(option);


var timeSeries = echarts.init(document.getElementById('timeSeries'));
ts_data = [['2017-04-23', 1],
    ['2017-04-30', 0],
    ['2017-05-07', 1],
    ['2017-05-14', 1],
    ['2017-05-21', 0],
    ['2017-05-28', 1],
    ['2017-06-04', 3],
    ['2017-06-11', 1],
    ['2017-06-18', 2],
    ['2017-06-25', 3],
    ['2017-07-02', 3],
    ['2017-07-09', 5],
    ['2017-07-16', 2],
    ['2017-07-23', 1],
    ['2017-07-30', 2],
    ['2017-08-06', 3],
    ['2017-08-13', 1],
    ['2017-08-20', 2],
    ['2017-08-27', 4],
    ['2017-09-03', 3],
    ['2017-09-10', 2],
    ['2017-09-17', 4],
    ['2017-09-24', 1],
    ['2017-10-01', 2],
    ['2017-10-08', 4],
    ['2017-10-15', 9],
    ['2017-10-22', 14],
    ['2017-10-29', 18],
    ['2017-11-05', 16],
    ['2017-11-12', 23],
    ['2017-11-19', 29],
    ['2017-11-26', 20],
    ['2017-12-03', 18],
    ['2017-12-10', 15],
    ['2017-12-17', 9],
    ['2017-12-24', 30],
    ['2017-12-31', 15],
    ['2018-01-07', 24],
    ['2018-01-14', 31],
    ['2018-01-21', 21],
    ['2018-01-28', 15],
    ['2018-02-04', 25],
    ['2018-02-11', 12],
    ['2018-02-18', 16],
    ['2018-02-25', 13],
    ['2018-03-04', 10],
    ['2018-03-11', 17],
    ['2018-03-18', 22],
    ['2018-03-25', 17],
    ['2018-04-01', 15],
    ['2018-04-08', 17],
    ['2018-04-15', 18],
    ['2018-04-22', 20],
    ['2018-04-29', 20],
    ['2018-05-06', 16],
    ['2018-05-13', 9],
    ['2018-05-20', 20],
    ['2018-05-27', 19],
    ['2018-06-03', 19],
    ['2018-06-10', 13],
    ['2018-06-17', 20],
    ['2018-06-24', 11],
    ['2018-07-01', 17],
    ['2018-07-08', 11],
    ['2018-07-15', 17],
    ['2018-07-22', 14],
    ['2018-07-29', 19],
    ['2018-08-05', 11],
    ['2018-08-12', 8],
    ['2018-08-19', 14],
    ['2018-08-26', 20],
    ['2018-09-02', 12],
    ['2018-09-09', 19],
    ['2018-09-16', 17],
    ['2018-09-23', 10],
    ['2018-09-30', 12],
    ['2018-10-07', 20],
    ['2018-10-14', 15],
    ['2018-10-21', 18],
    ['2018-10-28', 17],
    ['2018-11-04', 3],
    ['2018-11-11', 12],
    ['2018-11-18', 12],
    ['2018-11-25', 19],
    ['2018-12-02', 13],
    ['2018-12-09', 23],
    ['2018-12-16', 15],
    ['2018-12-23', 22],
    ['2018-12-30', 14],
    ['2019-01-06', 16],
    ['2019-01-13', 19],
    ['2019-01-20', 29],
    ['2019-01-27', 18],
    ['2019-02-03', 19],
    ['2019-02-10', 29],
    ['2019-02-17', 23],
    ['2019-02-24', 29],
    ['2019-03-03', 24],
    ['2019-03-10', 24],
    ['2019-03-17', 17],
    ['2019-03-24', 21],
    ['2019-03-31', 26],
    ['2019-04-07', 18],
    ['2019-04-14', 23],
    ['2019-04-21', 22],
    ['2019-04-28', 15],
    ['2019-05-05', 6],
    ['2019-05-12', 12],
    ['2019-05-19', 19],
    ['2019-05-26', 26],
    ['2019-06-02', 30],
    ['2019-06-09', 23],
    ['2019-06-16', 16],
    ['2019-06-23', 11],
    ['2019-06-30', 16],
    ['2019-07-07', 26],
    ['2019-07-14', 34],
    ['2019-07-21', 18],
    ['2019-07-28', 17],
    ['2019-08-04', 19],
    ['2019-08-11', 15],
    ['2019-08-18', 15],
    ['2019-08-25', 12],
    ['2019-09-01', 13],
    ['2019-09-08', 16],
    ['2019-09-15', 14],
    ['2019-09-22', 16],
    ['2019-09-29', 18],
    ['2019-10-06', 7],
    ['2019-10-13', 10],
    ['2019-10-20', 12],
    ['2019-10-27', 12],
    ['2019-11-03', 9],
    ['2019-11-10', 14],
    ['2019-11-17', 8],
    ['2019-11-24', 16],
    ['2019-12-01', 10],
    ['2019-12-08', 18],
    ['2019-12-15', 7],
    ['2019-12-22', 15],
    ['2019-12-29', 8]]

ts_mark_area = [
    [{
        xAxis: '2017-09-24'
    }, {
        xAxis: '2017-12-17'
    }],
    [{
        xAxis: '2017-12-31'
    }, {
        xAxis: '2018-01-28'
    }],
    [{
        xAxis: '2018-05-13'
    }, {
        xAxis: '2018-06-10'
    }],]
option = {
    grid: {
        left: '3%',
        right: '3%',
        top: '13%',
        bottom: '15%',
    },
    legend: {
        textStyle: {
            fontSize: 30,
            top: 20
        },
        icon: 'none'
    },
    dataZoom: {
        start: 10,
        end: 70,
    },
    xAxis: [
        {
            position: "bottom",
            type: 'category',
            boundaryGap: false,
            nameRotate: 1,
            splitLine: {
                show: false
            },
            // data: ['Jan-2017', 'Jan-2017', 'Jan-2017', 'Feb-2017', 'Mar-2017', 'Apr-2017', 'May-2017', 'Jun-2017', 'Jul-2017', 'Aug-2017', 'Sep-2017', 'Oct-2017', 'Nov-2017', 'Dec-2017', 'Feb-2018', 'Mar-2018', 'Apr-2018', 'Mai-2018', 'Jun-2018', 'Jun-2018']
        },
        {
            position: "bottom",
            offset: 25,
            zlevel: 1,
            type: 'category',
            boundaryGap: false,
            nameRotate: 2,
            splitLine: {
                show: false
            },
            axisLine: {
                show: false
            },
            axisTick: {
                show: false
            },
            data: ['May-2017', 'Dec-2019']
        }
    ],
    yAxis: {
        name: "#customers",
        type: 'value'
    },
    series: [{
        name: 'Weekly purchases of Hand Sanitizer by 120 Males, <35 ',
        data: ts_data,
        type: 'line',
        smooth: false,
        markArea: {
            itemStyle: {
                opacity: 0.2
            },
            data: ts_mark_area,
        }
    },]
};

timeSeries.setOption(option);
timeSeries.resize();


$(window).on('resize', function () {
    sankeyChart.resize();
    timeSeries.resize();
});

timeSeries.resize();
