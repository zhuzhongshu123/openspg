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
from knext.api.operator import LinkOp, PromptOp
from knext.operator.spg_record import SPGRecord
from knext.client.search import SearchClient


class IndicatorLinkOp(LinkOp):
    bind_to = "Indicator"

    def __init__(self):
        self.prompt_op = PromptOp.by_name("IndicatorLinkPrompt")
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

    def invoke(self, property: str, subject_record: SPGRecord) -> List[SPGRecord]:
        # Retrieve relevant indicators from KG based on indicator name
        name = property
        query = {"match": {"name": name}}
        recall_records = self.search_client.search(query, start=0, size=10)
        # Reranking the realled records with LLM to get final linking result
        data = {
            "input": name,
            "candidates": [x.get_property("name") for x in recall_records],
        }
        link_input = self.prompt_op.build_prompt(data)
        link_result = self.generate(link_input)
        return self.prompt_op.parse_response(link_result)
