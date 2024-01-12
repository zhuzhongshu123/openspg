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
import os
import requests
from typing import List
from knext.api.operator import FuseOp
from knext.api.record import SPGRecord
from knext.api.client import SearchClient

from schema.finance_schema_helper import Finance


class StateFuse(FuseOp):
    bind_to = "Finance.State"

    def __init__(self):
        super().__init__()
        from builder.operator.prompts import StateFusePrompt

        self.prompt = StateFusePrompt()
        from nn4k.invoker import NNInvoker

        pwd = os.path.dirname(__file__)
        self.invoker = NNInvoker.from_config(
            os.path.join(pwd, "../model/openai_infer.json")
        )
        self.state_search_client = SearchClient("Finance.State")

    def generate(self, input_data):
        return self.invoker.remote_inference(input_data)[0]

    def invoke(self, subject_records: List[SPGRecord]) -> List[SPGRecord]:
        print("Enter StateFuseOp===========================")
        print(f"subject_records = {subject_records}")
        fused_records = []
        for record in subject_records:
            recalled_states = self.state_search_client.fuzzy_search(
                record, "name", size=10
            )
            if len(recalled_states) == 0:
                print("no state recalled...")
                fused_records.append(subject_records)
            else:
                input_state = record.get_property("name")
                candidate_states = [x.get_property("name") for x in recalled_states]
                candidate_state_map = {}
                for state in recalled_states:
                    candidate_state_map[state.get_property("name")] = state
                content = (
                    f'input_state："{input_state}"，candidate_states：{candidate_states}'
                )
                prompt_input = {"input": content}
                print(f"req = {prompt_input}")
                rsp = self.generate(self.prompt.build_prompt(prompt_input))
                print(f"rsp = {rsp}")
                output = self.prompt.parse_response(rsp)
                if len(output) > 0:
                    output_state = candidate_state_map.get(output[0], None)
                    if output_state is None:
                        fused_records.append(record)
                    else:
                        # same state found sucessfully
                        fused_records.append(output_state)
        print(f"fused_records = {fused_records}")
        print("Exit StateFuseOp===========================")
        if len(fused_records) == 0:
            return subject_records
        return fused_records
