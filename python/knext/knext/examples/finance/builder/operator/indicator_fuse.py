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
from knext.api.operator import FuseOp
from knext.api.record import SPGRecord
from knext.api.client import SearchClient

from schema.finance_schema_helper import Finance


class IndicatorFuseOp(FuseOp):
    bind_to = Finance.Indicator

    def __init__(self):
        super().__init__()
        self.search_client = SearchClient(Finance.Indicator)

    def link(self, subject_record: SPGRecord) -> SPGRecord:
        # Retrieve relevant indicators from KG based on indicator name
        print("Enter IndicatorFuseOp.link===========================")        
        recall_records = self.search_client.fuzzy_search(subject_record, "name", size=1)
        
        print(f"subject: {subject_record}, linked: {recall_records}")
        print("Exit IndicatorFuseOp.link===========================")
        if len(recall_records) == 0:
            return None
        return recall_records[0]

    def merge(self, subject_record: SPGRecord, linked_record: SPGRecord) -> SPGRecord:
        if linked_record is None:
            return subject_record
        else:
            return linked_record
