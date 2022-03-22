// function demoOption 不要复制 ！！！
function demoOption(data, theme, echartsInstance, echarts) {

    var data_array = new Array()

    for (let i = 0; i < data.series[0].fields[0].values.length; i++) {
        var tz_value = data.series[0].fields[0].values.buffer[i]
        var tz_value_int = parseInt(tz_value)
        if (tz_value_int == 0) {
            tz_value_str = "中时区"
        } else if (tz_value_int > 0) {
            tz_value_str = '东' + tz_value + '区'
        } else if (tz_value_int < 0) {
            tz_value_str = '西' + (-tz_value) + '区'
        }
        data_array.push({
            name: tz_value_str,
            value: data.series[0].fields[1].values.buffer[i]
        })
    }

    option = {
        backgroundColor: 'transparent',
        title: {
            text: '代码库代码提交时区',
            left: 'center',
            top: 'center',
            textStyle: {
                color: '#103667'
            }
        },
        tooltip: {
            trigger: 'item',
            formatter: "{a} <br/>{b};<br/>提交量:{c}<br/>百分比:({d}%)"
        },
        legend: {
            orient: 'vertical',
            left: 'left',
            top: '20%',
            textStyle: {
                color: '#103667'
            }

        },
        series: [
            {
                name: '时区详细信息',
                type: 'pie',
                label: {
                    normal: {
                        formatter: '{b} 提交量:{c} 百分比:({d}%)',
                        textStyle: {
                            fontWeight: 'normal',
                            fontSize: 10
                        }
                    }
                },
                itemStyle: {
                    emphasis: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
                },
                radius: ['40%', '70%'],
                data: data_array
            }
        ]
    }
    return option
}