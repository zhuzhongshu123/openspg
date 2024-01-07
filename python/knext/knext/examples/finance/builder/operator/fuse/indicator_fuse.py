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
import requests
from typing import List
from knext.api.operator import FuseOp, PromptOp
from knext.operator.spg_record import SPGRecord
from knext.client.search import SearchClient


class IndicatorFuseOp(FuseOp):
    bind_to = "Indicator"

    def __init__(self):
        self.prompt_op = PromptOp.by_name("IndicatorFusePrompt")
        self.search_client = SearchClient("Indicator")

    def generate(self, input_data):
        req = {
            "input": input_data,
            "max_input_len": 1024,
            "max_output_len": 1024,
        }
        url = "http://11.166.207.228:9999/generate"
        try:
            rsp = requests.post(url, req)
            rsp.raise_for_status()
            return rsp.json()
        except Exception as e:
            return {"output": ""}

    def link(self, subject_record: SPGRecord) -> List[SPGRecord]:
        # Retrieve relevant indicators from KG based on indicator name
        name = subject_record.get_property("name")
        query = {"match": {"name": name}}
        recall_records = self.search_client.search(query, start=0, size=10)
        return recall_records

    def merge(
        self, subject_record: SPGRecord, linked_records: List[SPGRecord]
    ) -> List[SPGRecord]:
        # Merge the recalled indicators with LLM
        data = {
            "name": subject_record.get_property("name"),
            "candidates": [x.get_property("name") for x in linked_records],
        }
        merge_input = self.prompt_op.build_prompt(data)
        merge_result = self.generate(merge_input)
        merge_result = self.prompt_op.parse_response(merge_result)
        # If the KG already contains `subject_record`, return the existing record
        # (you can also update the properties of existing record as well),
        # otherwise return `subject_record`

        if merge_result is not None:
            return self.prompt_op.parse_response(merge_result)
        else:
            return [subject_record]
