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
import json
import requests
from typing import List
from knext.api.operator import PredictOp
from knext.api.record import SPGRecord
from knext.api.client import SearchClient

from schema.finance_schema_helper import Finance


class IndicatorPredictOp(PredictOp):
    bind_to = (Finance.Indicator, "isA", Finance.Indicator)

    def __init__(self):
        super().__init__()
        from builder.operator.prompts import IndicatorPredictPrompt

        self.prompt_op = IndicatorPredictPrompt()
        self.search_client = SearchClient(Finance.Indicator)

    def generate(self, input_data):
        # Request LLM to get hypernym predictions
        req = {
            "input": input_data,
            "max_input_len": 1024,
            "max_output_len": 1024,
        }
        url = "http://11.166.207.228:8888/generate"
        try:
            rsp = requests.post(url, req)
            rsp.raise_for_status()
            output = json.dumps(rsp.json()["output"][0])
            return output
        except Exception as e:
            print(f"faied to generate, info: {e}")

    def _recall(self, indicator):
        recall_records = self.search_client.fuzzy_search(indicator, "name", size=1)
        if len(recall_records) == 0:
            return None
        else:
            record = SPGRecord(
                Finance.Indicator,
            )
            record.upsert_properties(
                {
                    "id": recall_records[0].properties["name"],
                    "name": recall_records[0].properties["name"],
                }
            )

            return record

    def invoke(self, subject_record: SPGRecord) -> List[SPGRecord]:
        # Predict the hypernym indicators with LLM based on the indicator name. For example:
        # 一般公共预算收入-税收收入-增值税-土地增值税
        print("Enter IndicatorPredictOp===========================")
        name = subject_record.get_property("name")
        data = {"input": name}
        predict_input = self.prompt_op.build_prompt(data)
        predict_result = self.generate(predict_input)
        print(f"model_output = {predict_result}")
        if predict_result is None:
            return []
        predict_result = self.prompt_op.parse_response(predict_result)
        print(f"parsed_record = {predict_result}")
        output = []
        if len(predict_result) == 0:
            return output
        for item in predict_result:
            recalled_record = self._recall(item)
            print(item, recalled_record)
            if recalled_record is not None:
                output.append(recalled_record)
        print("Exit IndicatorPredictOp===========================")
        return output


if __name__ == "__main__":
    op = IndicatorPredictOp()
    subject_record = SPGRecord("Finance.Indicator")
    subject_record.upsert_property("name", "土地出让金")
    subject_record.upsert_property("id", "土地出让金")
