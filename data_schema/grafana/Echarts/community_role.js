// function demoOption 不要复制 ！！！
function demoOption(data, theme, echartsInstance, echarts) {

    var name_max_array = new Array()
    var data_value_array = new Array()
    for (let i = 4; i < 10; i++) {
     data_value_array.push(data.series[0].fields[i].values.buffer[0])
     name_max_array.push({
            name: data.series[0].fields[i].name,
            max: data.series[0].fields[2*i+2].values.buffer[0],
            min: data.series[0].fields[2*i+3].values.buffer[0]
        })
    }

    option = {
      title: {
        text: '社区角色雷达'
      },
      legend: {
        data: [data.series[0].fields[3].values.buffer[0]]
      },
      radar: {
        // shape: 'circle',
        indicator: name_max_array
      },
      series: [
        {
          name: 'Budget vs spending',
          type: 'radar',
          data: [
            {
              value: data_value_array,
              name: data.series[0].fields[3].values.buffer[0]
            }
          ]
        }
      ]
    }
    return option

}
