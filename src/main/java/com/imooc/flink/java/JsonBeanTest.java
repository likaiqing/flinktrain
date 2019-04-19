package com.imooc.flink.java;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class JsonBeanTest {

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        JsonNode node = mapper.readTree(json);
        JsonNode data = node.get("data");
        if (data.isArray()) {
            Iterator<JsonNode> it = data.iterator();
            while (it.hasNext()) {
                BtcUtxo btcUtxo = mapper.convertValue(it.next(), BtcUtxo.class);
//                System.out.println(btcUtxo);
            }
            JavaType beanList = mapper.getTypeFactory().constructParametricType(ArrayList.class, BtcUtxo.class);
            ArrayList<BtcUtxo> list = mapper.convertValue(data, beanList);
            for (BtcUtxo btcUtxo : list) {
                System.out.println(btcUtxo);
            }
            System.out.println("over");
        }
    }

    static class BtcUtxo {
        private String id;
        private String address;
        private String blockHeight;
        private String blocktime;
        private String txhash;
        private String voutIndex;////第几个输出
        private String valueSat;////金额，最小单位,聪
        private String value;//比特币单位，1btc=10^8 value_sat
        private String scriptHex;
        private String isFromCoinbase;
        private String createTime;
        private String updateTime;

        @Override
        public String toString() {
            return "RedisSinkWithWaterMarker{" +
                    "id='" + id + '\'' +
                    ", address='" + address + '\'' +
                    ", blockHeight='" + blockHeight + '\'' +
                    ", blocktime='" + blocktime + '\'' +
                    ", txhash='" + txhash + '\'' +
                    ", voutIndex='" + voutIndex + '\'' +
                    ", valueSat='" + valueSat + '\'' +
                    ", value='" + value + '\'' +
                    ", scriptHex='" + scriptHex + '\'' +
                    ", isFromCoinbase='" + isFromCoinbase + '\'' +
                    ", createTime='" + createTime + '\'' +
                    ", updateTime='" + updateTime + '\'' +
                    '}';
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getBlockHeight() {
            return blockHeight;
        }

        public void setBlockHeight(String blockHeight) {
            this.blockHeight = blockHeight;
        }

        public String getBlocktime() {
            return blocktime;
        }

        public void setBlocktime(String blocktime) {
            this.blocktime = blocktime;
        }

        public String getTxhash() {
            return txhash;
        }

        public void setTxhash(String txhash) {
            this.txhash = txhash;
        }

        public String getVoutIndex() {
            return voutIndex;
        }

        public void setVoutIndex(String voutIndex) {
            this.voutIndex = voutIndex;
        }

        public String getValueSat() {
            return valueSat;
        }

        public void setValueSat(String valueSat) {
            this.valueSat = valueSat;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getScriptHex() {
            return scriptHex;
        }

        public void setScriptHex(String scriptHex) {
            this.scriptHex = scriptHex;
        }

        public String getIsFromCoinbase() {
            return isFromCoinbase;
        }

        public void setIsFromCoinbase(String isFromCoinbase) {
            this.isFromCoinbase = isFromCoinbase;
        }

        public String getCreateTime() {
            return createTime;
        }

        public void setCreateTime(String createTime) {
            this.createTime = createTime;
        }

        public String getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(String updateTime) {
            this.updateTime = updateTime;
        }
    }

    static String json = "{\n" +
            "    \"data\": [\n" +
            "        {\n" +
            "            \"id\": \"3037608\",\n" +
            "            \"address\": \"1FqMzvoJQsuzKBKkTtf4bxCPdiaSpKvk8k\",\n" +
            "            \"block_height\": \"553532\",\n" +
            "            \"blocktime\": \"1544625118\",\n" +
            "            \"txhash\": \"f5d27fa80aa91426368854f1119c08bfab891fd6d9a2260b8fdc4f14752cdfae\",\n" +
            "            \"vout_index\": \"11\",\n" +
            "            \"value_sat\": \"0\",\n" +
            "            \"value\": \"0.0\",\n" +
            "            \"script_hex\": null,\n" +
            "            \"is_from_coinbase\": \"0\",\n" +
            "            \"create_time\": \"1554864429522\",\n" +
            "            \"update_time\": \"1554864429522\"\n" +
            "        }\n" +
            "    ],\n" +
            "    \"database\": \"okcoin_blockchain_btc\",\n" +
            "    \"es\": 1554865442000,\n" +
            "    \"id\": 128822,\n" +
            "    \"isDdl\": false,\n" +
            "    \"mysqlType\": {\n" +
            "        \"id\": \"bigint(20) unsigned\",\n" +
            "        \"address\": \"varchar(62)\",\n" +
            "        \"block_height\": \"int(10) unsigned\",\n" +
            "        \"blocktime\": \"bigint(20) unsigned\",\n" +
            "        \"txhash\": \"varchar(64)\",\n" +
            "        \"vout_index\": \"int(10) unsigned\",\n" +
            "        \"value_sat\": \"bigint(20) unsigned\",\n" +
            "        \"value\": \"double unsigned\",\n" +
            "        \"script_hex\": \"text\",\n" +
            "        \"is_from_coinbase\": \"tinyint(1)\",\n" +
            "        \"create_time\": \"bigint(20) unsigned\",\n" +
            "        \"update_time\": \"bigint(20) unsigned\"\n" +
            "    },\n" +
            "    \"old\": null,\n" +
            "    \"pkNames\": null,\n" +
            "    \"sql\": \"\",\n" +
            "    \"sqlType\": {\n" +
            "        \"id\": -5,\n" +
            "        \"address\": 12,\n" +
            "        \"block_height\": 4,\n" +
            "        \"blocktime\": -5,\n" +
            "        \"txhash\": 12,\n" +
            "        \"vout_index\": 4,\n" +
            "        \"value_sat\": -5,\n" +
            "        \"value\": 8,\n" +
            "        \"script_hex\": -4,\n" +
            "        \"is_from_coinbase\": -7,\n" +
            "        \"create_time\": -5,\n" +
            "        \"update_time\": -5\n" +
            "    },\n" +
            "    \"table\": \"btc_unspent_utxo_3\",\n" +
            "    \"ts\": 1555055866119,\n" +
            "    \"type\": \"INSERT\"\n" +
            "}";
}
