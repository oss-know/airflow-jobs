// function demoOption 不要复制 ！！！
function demoOption(data, theme, echartsInstance, echarts) {

    option1 ={
  title: {
    text: '请输入有效用户',
     textStyle:{
          color:'#103667'
        }
  },
    legend: {
    data: ['']
  },
  radar: {
    // shape: 'circle',
    indicator: [

      { name: 'knowledge_sharing', max: 6500 },
      { name: 'code_contribution', max: 16000 },
      { name: 'issue_coordination', max: 30000 },
      { name: 'progress_control', max: 38000 },
      { name: 'code_tweaking', max: 52000 },
      { name: 'issue_reporting', max: 25000 }
    ]
  },
  series: [
    {
      name: '请输入有效用户',
      type: 'radar',
      color:['#103667'],
      data: [
        {
          value: [6500, 16000, 30000, 38000, 52000, 25000],
          name: '请输入有效用户'
        }
      ]
    }
  ]
}
if(data.series[0]==undefined){
  return option1
}
var user_login = "用户登录名: "+data.series[0].fields[3].values.buffer[0]
var name_max_array = new Array()
    var data_value_array = new Array()
    for (let i = 4; i < 10; i++) {
      console.log(data.series[0].fields[i].values.buffer[0])
      console.log(data.series[0].fields[i].name)
      console.log(data.series[0].fields[2*i+2].values.buffer[0])
      console.log(data.series[0].fields[2*i+3].values.buffer[0])
     data_value_array.push(data.series[0].fields[i].values.buffer[0])
     name_max_array.push({
            name: data.series[0].fields[i].name,
            max: data.series[0].fields[2*i+2].values.buffer[0],
            min: data.series[0].fields[2*i+3].values.buffer[0]
        })
    }

    option = {
      title: {
        text: '社区角色雷达',
        textStyle:{
          color:'#103667'
        }
      },
      legend: {
        data: [user_login],
        left:'auto',
        top:'middle',
        textStyle:{
          color:'#103667'
        }

      },
      radar: {
         //shape: 'circle',
        //shape:'polygon',
        indicator: name_max_array
      },
      series: [
        {
          name: '社区角色雷达',
          type: 'radar',
          color:['#103667'],
          data: [
            {
              value: data_value_array,
              name: user_login
            }
          ]
        }
      ]
    }
    return option

}
