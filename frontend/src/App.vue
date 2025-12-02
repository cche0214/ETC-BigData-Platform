<template>
  <div style="padding: 20px">
    <h2>🚗 实时交通监测数据</h2>
    <button @click="fetchTraffic">获取 HBase 数据</button>

    <table v-if="rows.length" border="1" cellpadding="8" style="margin-top: 20px;">
  <thead>
    <tr>
      <th>RowKey</th>
      <th>行政区</th>
      <th>卡口名称</th>
      <th>方向类型</th>
      <th>过车时间</th>
      <th>号牌种类</th>
      <th>号牌号码</th>
      <th>车辆品牌型号号</th>
    </tr>
  </thead>
  <tbody>
    <tr v-for="r in rows" :key="r.rowkey">
      <td>{{ r.rowkey }}</td>
      <td>{{ r.XZQHMC }}</td>
      <td>{{ r.KKMC }}</td>
      <td>{{ r.FXLX }}</td>
      <td>{{ r.GCSJ }}</td>
      <td>{{ r.HPZL }}</td>
      <td>{{ r.HPHM }}</td>
      <td>{{ r.CLPPXH }}</td>
    </tr>
  </tbody>
</table>

  </div>
</template>

<script setup>
import { ref } from "vue"
import axios from "axios"

const rows = ref([])

// 调用 Flask 接口
async function fetchTraffic() {
  try {
    const res = await axios.get("/api/hbase/traffic")
    rows.value = res.data.data
  } catch (e) {
    alert("获取数据失败：" + e)
  }
}
</script>

<style>
table {
  border-collapse: collapse;
}
th {
  background: #f2f2f2;
}
button {
  padding: 6px 12px;
  background: #42b983;
  color: white;
  border: none;
  cursor: pointer;
  border-radius: 6px;
}
</style>
