# -*- coding: utf-8 -*-

# Copyright 2023 Ant Group CO., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.
import copy
import json
from typing import List, Dict
from knext.api.operator import PromptOp
from knext.api.record import SPGRecord


class IndicatorNERPrompt(PromptOp):
    template = {
        "input": "",
        "instruction": '参考example给出的示例，从input中抽取出符合schema定义的指标，如果不存在则返回空。请按照JSON字符串的格式回答。输出格式为:{"indicator":[{"type":"XXX", "name:"XXX"}]}',
        "example": {
            "input": "2022年，全国一般公共预算收入203703亿元，比上年增长0.6%，扣除留抵退税因素后增长9.1%",
            "output": '{"indicator": [{"type": "财政", "name": "全国一般公共预算收入"}]}',
        },
        "schema": {
            "type": "类型为文本，表示该指标所述的大类，如财政，债务等",
            "name": "类型为文本，代表指标名称，如土地出让收入，GDP，税收收入等",
        },
    }

    def build_prompt(self, variables: Dict[str, str]):
        tmp = copy.deepcopy(self.template)
        tmp["input"] = variables["input"]
        return json.dumps(tmp)

    def parse_response(self, response: str) -> List[SPGRecord]:
        try:
            records = json.loads(response)
        except Exception as e:
            print(f"failed to load {response}, info: {e}")
            return []

        output = []
        for record in records["indicator"]:
            print(f"extracted indicator: {record}")
            name = record.get("name", "")
            if len(name) == 0:
                continue
            tmp = SPGRecord("Finance.Indicator")
            tmp.upsert_property("id", name)
            tmp.upsert_property("name", name)
            output.append(tmp)
        return output


class IndicatorLogicPrompt(PromptOp):
    template = {
        "input": "",
        "instruction": '参考example给出的示例，从input中抽取出符合schema定义的指标状态与对应指标，如果不存在则返回空。请按照JSON字符串的格式回答。输出格式为:{"logic":[{"state":"XXX","indicator:"XXX"}]}',
        "example": {
            "input": '2022年，全国一般公共预算收入203703亿元，比上年增长0.6%，扣除留抵退税因素后增长9.1%\nNER: {"全国一般公共预算收入"}',
            "output": '{"logic":[{"state":"全国一般公共预算收入增长",indicator:"全国一般公共预算收入"}]}',
        },
        "schema": {
            "state": "类型为文本，表示某个指标的状态，如全国一般公共预算收入增长，GDP大幅下降",
            "indicator": "类型为文本，表示subject包含的指标，如土地出让收入，GDP，税收收入等",
        },
    }

    def build_prompt(self, variables: Dict[str, str]):
        tmp = copy.deepcopy(self.template)
        tmp["input"] = variables["input"]
        return json.dumps(tmp)

    def parse_response(self, response: str) -> List[SPGRecord]:
        try:
            records = json.loads(response)
        except Exception as e:
            print(f"failed to load {response}, info: {e}")
            return []

        output = []
        for record in records["logic"]:
            print(f"extracted logic: {record}")
            indicator_name = record.get("indicator", "")
            state_name = record.get("state", "")
            if len(indicator_name) == 0 or len(state_name) == 0:
                continue
            indicator = SPGRecord("Finance.Indicator")
            indicator.upsert_property("id", indicator_name)
            indicator.upsert_property("name", indicator_name)
            output.append(indicator)

            state = SPGRecord("Finance.State")
            state.upsert_property("id", state_name)
            state.upsert_property("name", state_name)
            state.upsert_property("derivedFrom", indicator_name)
            output.append(state)
        return output


class IndicatorPredictPrompt(PromptOp):
    def build_prompt(self, variables: Dict[str, str]):
        return variables["input"]

    def parse_response(self, response: str) -> List[SPGRecord]:
        response = json.loads(response)
        output = []
        dedup = set()
        for k, v in response.items():
            for ud in v:
                u, d = ud
                if u in dedup or len(u) == 0:
                    continue
                dedup.add(u)
                tmp = SPGRecord("Finance.Indicator")
                tmp.upsert_property("name", u)
                output.append(tmp)
        return output


class StateFusePrompt(PromptOp):
    template = {
        "input": "",
        "instruction": '参考example给出的示例，从input文本中的candidate_states列表找到与input_state含义相同的状态，如果candidate_states与input_state与都不接近，那么相同状态为空字符串。请按照JSON字符串的格式回答。输出格式为:{"input_state":"XXX","same_state:"XXX"}',
        "example": {
            "input": 'input_state："土地出让收入大幅下降"，candidate_states：["土地出让收入下降"，"土地出让收入增长", "土地出让金下降50%"]',
            "output": '{"input_state": "土地出让收入大幅下降", "same_state": "土地出让收入下降"}',
        },
        "schema": {
            "input_state": "类型为文本，表示某个经济指标状态，如全国一般公共预算收入增长，GDP大幅下降",
            "same_state": "类型为文本，表示某个经济指标状态，且与input_state含义相同",
        },
    }

    def build_prompt(self, variables: Dict[str, str]):
        tmp = copy.deepcopy(self.template)
        tmp["input"] = variables["input"]
        return json.dumps(tmp)

    def parse_response(self, response: str) -> List[SPGRecord]:
        try:
            record = json.loads(response)
        except Exception as e:
            print(f"failed to load {response}, info: {e}")
            return []

        output = []
        same_state = record.get("same_state", "")
        if len(same_state) > 0:
            tmp = SPGRecord("Finance.State")
            tmp.upsert_property("name", same_state)
            output.append(tmp)
        return output
