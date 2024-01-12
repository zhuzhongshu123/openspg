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
from knext.client.model.builder_job import BuilderJob
from knext.component.builder import CSVReader, SPGTypeMapping, KGWriter
from knext.component.builder.mapping import LinkingStrategyEnum
from schema.finance_schema_helper import Finance

class State(BuilderJob):
    def build(self):
        source = CSVReader(
            local_path="./builder/job/data/state_edges.csv",
            columns=["id1", "rel", "id2"],
            start_row=2,
        )

        mapping1 = (
            SPGTypeMapping(spg_type_name=Finance.State)
            .add_property_mapping("id2", Finance.State.id)
            .add_property_mapping("id2", Finance.State.name)

        )

        mapping2 = (
            SPGTypeMapping(spg_type_name=Finance.State)
            .add_property_mapping("id1", Finance.State.id)
            .add_property_mapping("id1", Finance.State.name)
            .add_relation_mapping(
                "id2", "causes", Finance.State, LinkingStrategyEnum.IDEquals
            )
        )

        
        sink = KGWriter()

        return source >> [mapping1, mapping2] >> sink


    # def build(self):
    #     source = CSVReader(
    #         local_path="./builder/job/data/indicator_edges.csv",
    #         columns=["id1", "rel", "id2"],
    #         start_row=2,
    #     )

    #     mapping1 = (
    #         SPGTypeMapping(spg_type_name=Finance.Indicator)
    #         .add_property_mapping("id2", Finance.Indicator.id)
    #         .add_property_mapping("id2", Finance.Indicator.name)
    #     )

    #     mapping2 = (
    #         SPGTypeMapping(spg_type_name=Finance.Indicator)
    #         .add_property_mapping("id1", Finance.Indicator.id)
    #         .add_property_mapping("id1", Finance.Indicator.name)
    #         .add_relation_mapping(
    #             "id2", "isA", Finance.Indicator, LinkingStrategyEnum.IDEquals
    #         )
    #     )

    #     sink = KGWriter()

    #     return source >> [mapping1, mapping2] >> sink
    
