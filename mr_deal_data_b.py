# -*- coding: utf-8 -*-
import configparser
import inspect
import math
import os
import re
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
from matplotlib import pyplot as plt


class Config:
    def __init__(self, in_config_file):
        self.config = configparser.ConfigParser()
        self.config.read(in_config_file, encoding='UTF-8')
        # self.config.read(in_config_file, encoding='GBK')
        # WalkTour_flag = self.config.get('WalkTour', 'test_flag')
        # WeTest_flag = self.config.get('WeTest', 'test_flag')
        # wifi_bluetooth_flag = self.config.get('WIFI_BlueTooth', 'test_flag')
        self.data_type = ''
        self._get_data_type()

    def _get_data_type(self):
        test_flag_list = ['true', 'false']
        WalkTour_flag = self.config.get('WalkTour', 'is_enabled')
        WeTest_flag = self.config.get('WeTest', 'is_enabled')
        wifi_bluetooth_flag = self.config.get('WIFI_BlueTooth', 'is_enabled')
        # print('WalkTour_flag', WalkTour_flag)
        # print('type: ', type(WalkTour_flag))
        if 'true' == WalkTour_flag.lower():
            print_with_line_number('WalkTour配置生效，准备处理WalkTour数据')
            self.data_type = 'WalkTour'
        elif 'true' == WeTest_flag.lower():
            print_with_line_number('WeTest配置生效，准备处理WeTest数据')
            self.data_type = 'WeTest'
        elif 'true' == wifi_bluetooth_flag:
            print_with_line_number('WIFI_BlueTooth配置生效，准备处理WIFI_BlueTooth数据')
            self.data_type = 'WIFI_BlueTooth'
        else:
            if WalkTour_flag.lower() not in test_flag_list:
                self.data_type = '配置文件WalkTour，字段is_enabled填写错误，实际填写内容: ' + WalkTour_flag + '，期望内容：true或者false'
            elif WeTest_flag.lower() not in test_flag_list:
                self.data_type = '配置文件WeTest,字段is_enabled填写错误，实际填写内容: ' + WeTest_flag + '，期望内容：true或者false'
            elif wifi_bluetooth_flag.lower() not in test_flag_list:
                self.data_type = '配置文件WIFI_BlueTooth,字段is_enabled填写错误，实际填写内容: ' + wifi_bluetooth_flag + '，期望内容：true或者false'
            else:
                self.data_type = '配置文件is_enabled字段全是false，缺少true'

    def get_table_file(self):
        in_table_file = self.config.get(self.data_type, '45g_table_file')
        return in_table_file

    def get_char_file(self):
        in_table_file = self.config.get(self.data_type, 'zcy_chart_file')
        return in_table_file

    def get_ue_file(self):
        in_table_file = self.config.get(self.data_type, '45g_test_log')
        return in_table_file

    def get_wifi_bluetooth_file(self):
        in_wifi_bluetooth_file = self.config.get(self.data_type, 'wifi_bluetooth_file')
        return in_wifi_bluetooth_file

    def get_test_area(self):
        in_test_area = self.config.get(self.data_type, 'test_area')
        return in_test_area

    def get_lon_value(self):
        in_lon_value = self.config.get('Coordinates', 'lon_O')
        return float(in_lon_value)

    def get_lat_value(self):
        in_lat_value = self.config.get('Coordinates', 'lat_O')
        return float(in_lat_value)

    def get_len_x_value(self):
        in_len_x_value = self.config.get('Coordinates', 'len_east_x')
        return float(in_len_x_value)

    def get_len_y_value(self):
        in_len_y_value = self.config.get('Coordinates', 'len_north_y')
        return float(in_len_y_value)

    def get_device_brand(self):
        in_device_brand = self.config.get('SceneInfo', 'f_device_brand')
        return in_device_brand

    def get_device_model(self):
        in_device_model = self.config.get('SceneInfo', 'f_device_model')
        return in_device_model

    def get_area(self):
        in_area = self.config.get('SceneInfo', 'f_area')
        return in_area

    def get_floor(self):
        in_floor = self.config.get('SceneInfo', 'f_floor')
        return in_floor

    def get_scenario(self):
        in_scenario = self.config.get('SceneInfo', 'f_scenario')
        return in_scenario

    def get_district(self):
        in_district = self.config.get('SceneInfo', 'f_district')
        return in_district

    def get_street(self):
        in_street = self.config.get('SceneInfo', 'f_street')
        return in_street

    def get_building(self):
        in_building = self.config.get('SceneInfo', 'f_building')
        return in_building

    def get_source(self):
        in_floor = self.config.get('SceneInfo', 'f_source')
        return in_floor

    def get_province(self):
        in_scenario = self.config.get('SceneInfo', 'f_province')
        return in_scenario

    def get_city(self):
        in_district = self.config.get('SceneInfo', 'f_city')
        return in_district

    def get_prru_id(self):
        in_street = self.config.get('SceneInfo', 'f_prru_id')
        return in_street

    def get_msisdn(self):
        in_building = self.config.get('SceneInfo', 'f_msisdn')
        return in_building

    def get_data_type(self):
        in_building = self.config.get(self.data_type, 'data_type')
        return in_building


class Time:
    # 转换df中整列数据的时间格式
    @staticmethod
    def convert_time_format(in_time):
        tmp_list = []
        for i_t in in_time:
            in_input_datetime = datetime.strptime(str(i_t), "%Y%m%d%H%M%S%f")
            # 格式化为所需的字符串格式
            tmp_output_str = in_input_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            tmp_list.append(tmp_output_str)
        return tmp_list

    # 把秒数转成年月日时间
    @staticmethod
    def convert_seconds_to_datetime(timestamp):
        date_obj = pd.to_datetime(timestamp, unit='s')
        return date_obj.year, date_obj.month, date_obj.day

    @staticmethod
    def convert_datetime_to_seconds(dtime):
        return [time.mktime(time.strptime(x, '%Y-%m-%d %H:%M:%S.%f')) for x in dtime]

    @staticmethod
    def convert_datetime_to_seconds_slash(dtime):
        return [time.mktime(time.strptime(x, '%Y/%m/%d %H:%M:%S.%f')) for x in dtime]


class WeTest:
    @staticmethod
    def merge_ue_zcy_df_convert_format(in_ue_df, in_zcy_df):
        in_ue_df['pc_time'] = Time.convert_time_format(in_ue_df['pc_time'])

        in_ue_df = pd.merge(in_ue_df, in_zcy_df, left_on="pc_time", right_on="test_time", how='left')
        in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['pc_time'])
        return in_ue_df

    @staticmethod
    def merge_ue_zcy_df(in_ue_df, in_zcy_df):
        in_ue_df = pd.merge(in_ue_df, in_zcy_df, left_on="pc_time", right_on="test_time", how='left')
        if '-' in in_ue_df['pc_time'][0]:
            in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['pc_time'])
        else:
            in_ue_df['ts'] = Time.convert_datetime_to_seconds_slash(in_ue_df['pc_time'])
        return in_ue_df

    @staticmethod
    def outdoor_get_df(in_config):
        in_ue_file = in_config.get_ue_file()
        print_with_line_number(f'读取UE文件：{in_ue_file}')
        in_ue_df = Common.read_csv_get_df(in_ue_file)
        if '-' in in_ue_df['pc_time'][0]:
            in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['pc_time'])
        else:
            in_ue_df['ts'] = Time.convert_datetime_to_seconds_slash(in_ue_df['pc_time'])
        return in_ue_df

    @staticmethod
    def indoor_get_df(in_config, in_deal_zcy_char_csv_file):
        in_ue_file = in_config.get_ue_file()
        in_char_file = in_config.get_char_file()
        print_with_line_number(f'读取char文件：{in_char_file}')
        in_zcy_df = in_deal_zcy_char_csv_file(in_char_file)
        in_ue_df = Common.read_csv_get_df(in_ue_file)
        print_with_line_number(f'读取ue文件：{in_ue_file}')
        # if '-' not in str(in_zcy_df['test_time'])[0] and '/' not in str(in_zcy_df['test_time'])[0]:
        #     in_zcy_df['test_time'] = Time.convert_time_format(in_zcy_df['test_time'])
        if '-' not in str(in_ue_df['pc_time'])[0] and '/' not in str(in_ue_df['pc_time'])[0]:
            in_ue_df['pc_time'] = Time.convert_time_format(in_ue_df['pc_time'])

        res_zcy_ue_merge_df = WeTest.merge_ue_zcy_df(in_ue_df, in_zcy_df)

        return res_zcy_ue_merge_df

    @staticmethod
    def deal_wetest_5g(log_df_5g):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        # print('log_df_5g: ', log_df_5g.columns)
        log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()
        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'nr_neighbor_cell_{cell_cnt}_freq' in log_df_5g.columns:
                log_df_5g = log_df_5g.rename(
                    columns={
                        f'nr_neighbor_cell_{cell_cnt}_freq': f'f_freq_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_pci': f'f_pci_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_rsrp': f'f_rsrp_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_rsrq': f'f_rsrq_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_sinr': f'f_sinr_n{cell_cnt}',
                    })
            else:
                break

        log_df_5g = log_df_5g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'nci': 'f_cell_id',
                'ts': 'f_time',
                'nr_serving_cell_pci': 'f_pci',
                'nr_serving_cell_freq': 'f_freq',
                'nr_serving_cell_ssb_rsrp': 'f_rsrp',
                'nr_serving_cell_ssb_rsrq': 'f_rsrq',
                'nr_serving_cell_ssb_sinr': 'f_sinr',
            })
        # 删除重复列
        log_df_5g = Common.delete_duplicate_columns(log_df_5g)

        # 时间转上海时区
        sh_timez = Common.change_time_zone(log_df_5g['f_time'])
        log_df_5g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = Common.generate_finger_id(log_df_5g['f_time_1'], log_df_5g['f_msisdn'])
        log_df_5g['finger_id'] = finger_id
        # 获取领区数
        num_list = Common.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        # 置空 UEMR 数据
        log_df_5g = Common.fill_in_empty_header(log_df_5g)

        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(
            Time.convert_seconds_to_datetime).to_list()
        # log_df_5g['f_cst_time'] = log_df_5g['pc_time']
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)

        log_df_5g = log_df_5g.reindex(columns=OutDataTableFormat.WeTest5G)
        Common.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    @staticmethod
    def deal_wetest_4g(log_df_4g):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        Common.delete_df_duplicate_data_by_second(log_df_4g, log_df_4g['ts'])

        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_4g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_4g.columns]

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'lte_neighbor_cell_{cell_cnt}_freq' in log_df_4g.columns:
                log_df_4g = log_df_4g.rename(
                    columns={
                        f'lte_neighbor_cell_{cell_cnt}_freq': f'f_freq_n{cell_cnt}',
                        f'lte_neighbor_cell_{cell_cnt}_pci': f'f_pci_n{cell_cnt}',
                        f'lte_neighbor_cell_{cell_cnt}_rsrp': f'f_rsrp_n{cell_cnt}',
                        f'lte_neighbor_cell_{cell_cnt}_rsrq': f'f_rsrq_n{cell_cnt}',
                    })
            else:
                break

        # 列重命名
        log_df_4g = log_df_4g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'lte_eci': 'f_cell_id',
                'ts': 'f_time',
                'lte_serving_cell_pci': 'f_pci',
                'lte_serving_cell_freq': 'f_freq',
                'lte_serving_cell_rsrp': 'f_rsrp',
                'lte_serving_cell_rsrq': 'f_rsrq',
            })
        # 删除重复列
        log_df_4g = Common.delete_duplicate_columns(log_df_4g)

        # 时间转上海时区
        sh_timez = Common.change_time_zone(log_df_4g['f_time'])
        log_df_4g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = Common.generate_finger_id(log_df_4g['f_time_1'], log_df_4g['f_msisdn'])
        log_df_4g['finger_id'] = finger_id
        # 获取领区数
        num_list = Common.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = num_list
        # 置空 UEMR 数据
        log_df_4g = Common.fill_in_empty_header(log_df_4g)

        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256

        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(
            Time.convert_seconds_to_datetime).to_list()
        log_df_4g['f_eci'] = log_df_4g['f_cell_id']

        # SID暂时都赋值1
        log_df_4g['f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
        log_df_4g = log_df_4g.reindex(columns=OutDataTableFormat.WeTest4G)
        Common.data_filling(log_df_4g, 'f_cell_id')

        # # 标题统一小写
        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g


class WalkTour:
    # 获取zcy和wifi联合数据
    @staticmethod
    def indoor_get_df_wifi_bluetooth_zcy(in_config, in_deal_data):
        # 获取zcy，wifi和ue联合数据
        in_ue_file = in_config.get_ue_file()
        in_table_file = in_config.get_table_file()
        in_char_file = in_config.get_char_file()
        # 获取wifi 蓝牙文件
        in_wifi_bluetooth_file = in_config.get_wifi_bluetooth_file()
        print_with_line_number(f'获取UE文件：{in_ue_file}')
        print_with_line_number(f'获取table文件：{in_table_file}')
        print_with_line_number(f'获取char文件：{in_char_file}')
        print_with_line_number(f'获取wifi bluetooth文件：{in_wifi_bluetooth_file}')
        # 处理char数据
        tmp_zcy_df = in_deal_data.deal_zcy_char_csv_file(in_char_file)
        # 处理wifi 蓝牙
        tmp_wifi_bluetooth_df = in_deal_data.deal_wifi_bluetooth_csv_file(in_wifi_bluetooth_file)

        # 获取zcy，wifi 蓝牙合并数据
        tmp_merger_df = pd.merge(tmp_wifi_bluetooth_df, tmp_zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                 how='left')

        # 获取ue table 合并数据
        tmp_ue_table_df = WalkTour.get_merge_ue_table_data(in_ue_file, in_table_file)
        # 合并所有数据
        res_merge_df = WalkTour.merge_ue_zcy_data(tmp_ue_table_df, tmp_merger_df)
        return res_merge_df

    # 室外数据获取merge df
    @staticmethod
    def outdoor_get_df(in_config):
        ue_file = in_config.get_ue_file()
        table_file = in_config.get_table_file()
        print_with_line_number(f'读取ue文件：{ue_file}')
        print_with_line_number(f'读取table文件：{table_file}')
        rea_ue_table_df = WalkTour.get_merge_ue_table_data(ue_file, table_file)
        rea_ue_table_df['ts'] = Time.convert_datetime_to_seconds(rea_ue_table_df['PC Time'])
        return rea_ue_table_df

    # 室内数据获取merge df
    @staticmethod
    def indoor_get_df_zcy(in_config, in_deal_zcy_char_csv_file):
        in_ue_file = in_config.get_ue_file()
        in_table_file = in_config.get_table_file()
        in_char_file = in_config.get_char_file()
        tmp_zcy_df = in_deal_zcy_char_csv_file(in_char_file)
        print_with_line_number(f'读取UE文件：{in_ue_file}')
        print_with_line_number(f'读取table文件：{in_table_file}')
        print_with_line_number(f'读取char文件：{in_char_file}')
        tmp_ue_table_df = WalkTour.get_merge_ue_table_data(in_ue_file, in_table_file)
        res_zcy_ue_merge_df = WalkTour.merge_ue_zcy_data(tmp_ue_table_df, tmp_zcy_df)
        return res_zcy_ue_merge_df

    # 合并UE table数据
    @staticmethod
    def get_merge_ue_table_data(in_ue_file, in_table_file):
        in_ue_df = Common.read_csv_get_df(in_ue_file)
        if os.path.exists(in_table_file):
            in_table_df = Common.read_csv_get_df(in_table_file)
            res_tmp_merge_df = pd.merge(in_ue_df, in_table_df, left_on="PC Time", right_on="PCTime", how='left')
            return res_tmp_merge_df
        else:
            print('WalkTour数据中缺少table数据，请把table数据csv文件路径配置到配置文件中')
            return

    # 合并ue和zcy
    @staticmethod
    def merge_ue_zcy_data(in_ue_df, in_zcy_df):
        in_ue_df['ts'] = Time.convert_datetime_to_seconds(in_ue_df['PC Time'])
        in_zcy_df['ts'] = Time.convert_datetime_to_seconds(in_zcy_df['test_time'])
        tmp_df = pd.merge(in_ue_df, in_zcy_df)
        return tmp_df

    @staticmethod
    def deal_5g_df_data(log_df_5g, in_wifi_flag=False):
        # 删除测试log中 秒级重复数据，同秒取第一条
        log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。

        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        i = 0
        while True:
            i += 1
            if f'NCell{i} -Beam NARFCN' in log_df_5g.columns:
                log_df_5g = log_df_5g.rename(
                    columns={
                        f'NCell{i} -Beam NARFCN': f'f_freq_n{i}',
                        f'NCell{i} -Beam PCI': f'f_pci_n{i}',
                        f'NCell{i} -Beam SS-RSRP': f'f_rsrp_n{i}',
                        f'NCell{i} -Beam SS-RSRQ': f'f_rsrq_n{i}',
                        f'NCell{i} -Beam SS-SINR': f'f_sinr_n{i}',
                    })
            else:
                break

        log_df_5g = log_df_5g.rename(
            columns={
                'IMSI': 'f_imsi',
                'IMEI': 'f_imei',
                'NCI': 'f_cell_id',
                'ts': 'f_time',
                'PCell1 -Beam PCI': 'f_pci',
                'PCell1 -Beam NARFCN': 'f_freq',
                'PCell1 -Beam SS-RSRP': 'f_rsrp',
                'PCell1 -Beam SS-RSRQ': 'f_rsrq',
                'PCell1 -Beam SS-SINR': 'f_sinr',
                'PC Time': 'pc_time',
            })

        # 重命名zcy数据
        log_df_5g = log_df_5g.rename(
            columns={
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        # 删除重复行
        log_df_5g = Common.delete_duplicate_columns(log_df_5g)

        # 时间转上海时区
        sh_timez = Common.change_time_zone(log_df_5g['f_time'])
        log_df_5g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = Common.generate_finger_id(log_df_5g['f_time_1'], log_df_5g['f_msisdn'])
        log_df_5g['finger_id'] = finger_id
        # 置空 UEMR 数据
        log_df_5g = Common.fill_in_empty_header(log_df_5g)

        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(
            Time.convert_seconds_to_datetime).to_list()
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        if in_wifi_flag:
            out_data_table = OutDataTableFormat.WalkTour5G + OutDataTableFormat.WIFI_BlueTooth
        else:
            out_data_table = OutDataTableFormat.WalkTour5G

        log_df_5g = log_df_5g.reindex(columns=out_data_table)
        # 获取领区数
        num_list = Common.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        Common.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    @staticmethod
    def deal_4g_df_data(log_df_4g, in_wifi_flag=False):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_4g = Common.delete_df_duplicate_data_by_second(log_df_4g, log_df_4g['ts'])

        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_4g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_4g.columns]

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'NCell{cell_cnt} EARFCN' in log_df_4g.columns:
                log_df_4g = log_df_4g.rename(
                    columns={
                        f'NCell{cell_cnt} EARFCN': f'f_freq_n{cell_cnt}',
                        f'NCell{cell_cnt} PCI': f'f_pci_n{cell_cnt}',
                        f'NCell{cell_cnt} RSRP': f'f_rsrp_n{cell_cnt}',
                        f'NCell{cell_cnt} RSRQ': f'f_rsrq_n{cell_cnt}',
                    })
            else:
                break

        # 重命名table数据
        log_df_4g = log_df_4g.rename(
            columns={
                'IMSI': 'f_imsi',  # table
                'IMEI': 'f_imei',  # table
            })
        # 重命名zcy数据
        log_df_4g = log_df_4g.rename(
            columns={
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        # 重命名ue数据
        log_df_4g = log_df_4g.rename(
            columns={
                'PCell ECI': 'f_cell_id',
                'ts': 'f_time',
                'PCell PCI': 'f_pci',
                'PCell EARFCN': 'f_freq',
                'PCell RSRP': 'f_rsrp',
                'PCell RSRQ': 'f_rsrq',
                'PC Time': 'pc_time',
            })

        # 删除重复列
        log_df_4g = Common.delete_duplicate_columns(log_df_4g)

        # 时间转上海时区
        sh_timez = Common.change_time_zone(log_df_4g['f_time'])
        log_df_4g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = Common.generate_finger_id(log_df_4g['f_time_1'], log_df_4g['f_msisdn'])
        log_df_4g['finger_id'] = finger_id
        # 置空 UEMR 数据
        log_df_4g = Common.fill_in_empty_header(log_df_4g)

        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256
        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(
            Time.convert_seconds_to_datetime).to_list()

        log_df_4g['f_eci'] = log_df_4g['f_cell_id']

        # SID暂时都赋值1
        log_df_4g['f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)

        if in_wifi_flag:
            out_data_table = OutDataTableFormat.WalkTour4G + OutDataTableFormat.WIFI_BlueTooth
        else:
            out_data_table = OutDataTableFormat.WalkTour4G

        log_df_4g = log_df_4g.reindex(columns=out_data_table)
        # 计算领区数
        cell_number = Common.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = cell_number
        Common.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g


class OutDataTableFormat:
    WalkTour4G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                  'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                  'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                  'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                  'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                  'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr', 'f_neighbor_cell_number', 'f_freq_n1',
                  'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                  'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                  'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                  'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                  'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                  'f_sinr_n5',
                  'f_rsrp_n6', 'f_rsrq_n6', 'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                  'f_rsrq_n8', 'f_sinr_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                  'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                  'f_device_model']

    WalkTour5G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                  'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                  'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                  'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                  'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                  'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr', 'f_neighbor_cell_number', 'f_freq_n1',
                  'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                  'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                  'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                  'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                  'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                  'f_sinr_n5',
                  'f_rsrp_n6', 'f_rsrq_n6', 'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                  'f_rsrq_n8', 'f_sinr_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                  'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                  'f_device_model']

    WeTest4G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                'f_enb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                'f_freq', 'f_rsrp', 'f_rsrq', 'f_neighbor_cell_number', 'f_freq_n1',
                'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                'f_rsrp_n1', 'f_rsrq_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_rsrp_n3',
                'f_rsrq_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                'f_rsrp_n6', 'f_rsrq_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_rsrp_n8',
                'f_rsrq_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                'f_device_model']

    WeTest5G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr', 'f_neighbor_cell_number', 'f_freq_n1',
                'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                'f_sinr_n5',
                'f_rsrp_n6', 'f_rsrq_n6', 'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                'f_rsrq_n8', 'f_sinr_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                'f_device_model']

    WIFI_BlueTooth = ['f_wifi_name_1', 'f_wifi_mac_1', 'f_wifi_rssi_1',
                      'f_wifi_freq_1', 'f_wifi_name_2', 'f_wifi_mac_2', 'f_wifi_rssi_2',
                      'f_wifi_freq_2', 'f_wifi_name_3', 'f_wifi_mac_3', 'f_wifi_rssi_3',
                      'f_wifi_freq_3', 'f_wifi_name_4',
                      'f_wifi_mac_4', 'f_wifi_rssi_4', 'f_wifi_freq_4', 'f_wifi_name_5',
                      'f_wifi_mac_5', 'f_wifi_rssi_5', 'f_wifi_freq_5', 'f_wifi_name_6',
                      'f_wifi_mac_6', 'f_wifi_rssi_6', 'f_wifi_freq_6', 'f_wifi_name_7',
                      'f_wifi_mac_7', 'f_wifi_rssi_7', 'f_wifi_freq_7', 'f_wifi_name_8',
                      'f_wifi_mac_8', 'f_wifi_rssi_8', 'f_wifi_freq_8', 'f_bluetooth_mac_1',
                      'f_bluetooth_uuid_1', 'f_bluetooth_rssi_1', 'f_bluetooth_mac_2',
                      'f_bluetooth_uuid_2', 'f_bluetooth_rssi_2', 'f_bluetooth_mac_3',
                      'f_bluetooth_uuid_3', 'f_bluetooth_rssi_3', 'f_bluetooth_mac_4',
                      'f_bluetooth_uuid_4', 'f_bluetooth_rssi_4', 'f_bluetooth_mac_5',
                      'f_bluetooth_uuid_5', 'f_bluetooth_rssi_5', 'f_bluetooth_mac_6',
                      'f_bluetooth_uuid_6', 'f_bluetooth_rssi_6', 'f_bluetooth_mac_7',
                      'f_bluetooth_uuid_7', 'f_bluetooth_rssi_7', 'f_bluetooth_mac_8',
                      'f_bluetooth_uuid_8', 'f_bluetooth_rssi_8', ]


class Common:
    # 判断文件存在
    @staticmethod
    def check_file_exists(in_file):
        return os.path.exists(in_file)

    # 检查路径，如果不存在则创建
    @staticmethod
    def check_path(in_path):
        if not os.path.exists(in_path):
            # os.mkdir(in_path)
            os.makedirs(in_path, exist_ok=True)

    # wifi 蓝牙生成输出文件名称
    @staticmethod
    def wifi_bluetooth_generate_output_file_name(in_data_path, in_df, in_n_scene, in_test_type):
        # 时间转换
        name_d_time = in_df['test_time'][0].split(' ')[0]
        if '-' in name_d_time:
            name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
        else:
            name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')
        # print('name_d_time: ', name_d_time)

        district = in_df['f_district'][0]
        if '海淀' in district:
            n_are = 'HaiDian'
        elif '朝阳' in district:
            n_are = 'CaoYang'
        else:
            n_are = 'DaXin'
        # print(district)
        if in_n_scene:
            tmp_out_file_name = f'{n_are}_{in_n_scene}_{in_test_type}_data_{name_d_time}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'
        else:
            tmp_out_file_name = f'{n_are}_{in_test_type}_LOG_DT_UE_{name_d_time}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'

        tmp_out_file = os.path.join(in_data_path, tmp_out_file_name)
        # print('tmp_out_file: ', tmp_out_file)
        return tmp_out_file

    # 生成输出文件名称
    @staticmethod
    def generate_output_file_name(in_data_path, in_df, in_net_type, in_n_scene, in_test_type):

        name_d_time = in_df['pc_time'][0].split(' ')[0]
        if '-' in name_d_time:
            name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
        else:
            name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')
        # print('name_d_time: ', name_d_time)

        district = in_df['f_district'][0]
        if '海淀' in district:
            n_are = 'HaiDian'
        elif '朝阳' in district:
            n_are = 'CaoYang'
        else:
            n_are = 'DaXin'
        # print(district)
        if in_n_scene:
            tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{name_d_time}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'
        else:
            tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{name_d_time}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'

        tmp_out_file = os.path.join(in_data_path, tmp_out_file_name)
        # print('tmp_out_file: ', tmp_out_file)
        return tmp_out_file

    # 设置场景信息
    @staticmethod
    def set_scene_data(log_df, in_config):
        # 设置场景信息
        log_df['f_device_brand'] = in_config.get_device_brand()
        log_df['f_device_model'] = in_config.get_device_model()
        log_df['f_area'] = in_config.get_area()
        log_df['f_floor'] = in_config.get_floor()
        log_df['f_scenario'] = in_config.get_scenario()
        log_df['f_province'] = in_config.get_province()
        log_df['f_city'] = in_config.get_city()
        log_df['f_district'] = in_config.get_district()
        log_df['f_street'] = in_config.get_street()
        log_df['f_building'] = in_config.get_building()
        log_df['f_prru_id'] = in_config.get_prru_id()
        log_df['f_source'] = in_config.get_source()
        log_df['f_msisdn'] = in_config.get_msisdn()
        return log_df

    # 上填充方式，填充数据
    @staticmethod
    def data_filling(in_df, column_name):
        temp = np.nan
        index = []
        for i in range(len(in_df)):
            value = in_df.loc[i, column_name]  # 获取列每行的值
            if str(value) != 'nan':
                # 临时存储用于填充
                temp = in_df.loc[i, column_name]
            elif str(temp) == 'nan':
                # 将下标存储用于向上填充
                index.append(i)
            else:
                # 将临时变量的值进行填充
                in_df.loc[i, column_name] = temp
            if str(value) != 'nan':
                for k in index:
                    # 避免第一个为空的情况，向上填充
                    in_df.loc[k, column_name] = value
                index.clear()
        return in_df.reset_index(drop=True)

    # 获取领区数
    @staticmethod
    def get_cell_number(log_df):
        tmp_num_list = []
        for i in range(len(log_df)):
            tmp_num_list.append(np.count_nonzero(
                log_df[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5',
                        'f_rsrp_n6', 'f_rsrp_n7', 'f_rsrp_n8']].isnull().values[i] == False))
        return tmp_num_list

    # 填充空字符
    @staticmethod
    def fill_in_empty_header(log_df):
        log_df['f_roaming_type'] = ''
        log_df['f_phr'] = ''
        log_df['f_enb_received_power'] = ''
        log_df['f_ta'] = ''
        log_df['f_aoa'] = ''
        return log_df

    # 生成结果数据中得finger_id
    @staticmethod
    def generate_finger_id(time_df, msi_df):
        res_id = 'F' + time_df.dt.strftime('%Y%m%d%H') + '_' + msi_df.str[-4:]
        return res_id

    @staticmethod
    def change_time_zone(deal_df, in_timezone='Asia/Shanghai'):
        res_df = pd.to_datetime(deal_df, unit='s').dt.tz_localize(pytz.utc).dt.tz_convert(
            pytz.timezone(in_timezone))
        return res_df

    @staticmethod
    def delete_duplicate_columns(log_df):
        # 删除重复列
        log_df = log_df.loc[:, ~log_df.columns.duplicated()]
        return log_df

    @staticmethod
    def read_csv_get_df(in_df_path):
        in_df = pd.read_csv(in_df_path, low_memory=False)
        return in_df

    # 走测仪数据转经纬度
    @staticmethod
    def data_conversion(in_value, df_data):
        delta_d = df_data.max() - df_data.min()
        t_x = in_value / delta_d
        n_values = df_data * t_x
        res_dat = n_values - n_values.min()
        return res_dat

    # 走测仪数据生成png图片
    @staticmethod
    def generate_images(in_x1_v, in_y1_v, in_lon, in_lat, in_out_path, in_image_name):
        plt.clf()

        plt.rcParams['axes.unicode_minus'] = False
        plt.rcParams['font.sans-serif'] = 'SimHei'
        # Mac系统字体
        # plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
        plt.subplot(2, 1, 1)
        plt.plot(in_x1_v, in_y1_v)
        plt.gca().set_aspect('equal', adjustable='box')
        plt.xlabel('X')
        plt.ylabel('Y')
        plt.title('xy_相对位置轨迹图')
        png_file = os.path.join(in_out_path, f'xy_相对位置轨迹图{in_image_name}.png')
        plt.savefig(png_file)

        # 清除图形
        plt.clf()

        # plt.savefig(png_file)
        plt.subplot(2, 1, 2)
        plt.plot(in_lon, in_lat)
        plt.gca().set_aspect('equal', adjustable='box')
        plt.xlabel('lon')
        plt.ylabel('lat')
        plt.title('lonlat_经纬度轨迹图')
        plt.tight_layout()
        # plt.show()
        png_file = os.path.join(in_out_path, f'lonlat_经纬度轨迹图{in_image_name}.png')
        plt.savefig(png_file)

    @staticmethod
    def df_write_to_csv(w_df, w_file):
        w_df.to_csv(w_file, index=False)

    @staticmethod
    def delete_df_duplicate_data_by_second(df_dta, g_df):
        df_dta = df_dta.groupby(g_df).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
        return df_dta


class DealData:
    def __init__(self, in_config):
        self.config = in_config
        self.lon = self.config.get_lon_value()
        self.lat = self.config.get_lat_value()
        self.x = self.config.get_len_x_value()
        self.y = self.config.get_len_y_value()

    def set_config(self, in_config):
        self.config = in_config

    def deal_wifi_bluetooth_csv_file(self, in_wifi_bluetooth_file):
        zcy_path = os.path.join(os.path.dirname(in_wifi_bluetooth_file), 'zcy_data')
        Common.check_path(zcy_path)

        wifi_bluetooth_df = Common.read_csv_get_df(in_wifi_bluetooth_file)

        res_x1_values = Common.data_conversion(self.x, wifi_bluetooth_df['f_x'])
        lon = res_x1_values / (111000 * math.cos(self.lon / 180 * math.pi)) + self.lon
        lon = 2 * max(lon) - lon

        res_y1_values = Common.data_conversion(self.y, wifi_bluetooth_df['f_y'])
        lat = res_y1_values / 111000 + self.lat

        wifi_bluetooth_df['f_x'] = res_x1_values
        wifi_bluetooth_df['f_y'] = res_y1_values
        wifi_bluetooth_df['f_longitude'] = lon
        wifi_bluetooth_df['f_latitude'] = lat

        out_f = os.path.basename(in_wifi_bluetooth_file).split(".")[0] + f'_xyToLonLat_WIFI_BlueTooth.csv'
        Common.df_write_to_csv(wifi_bluetooth_df, os.path.join(zcy_path, out_f))

        Common.generate_images(res_x1_values, res_y1_values, lon, lat, zcy_path, '_wifi_蓝牙')

        # 赛选wifi 蓝牙数据
        res_wifi_bluetooth_df = wifi_bluetooth_df.drop(
            ['f_x', 'f_y', 'f_longitude', 'f_latitude', 'f_direction', 'f_altitude'], axis=1)
        return res_wifi_bluetooth_df

    def deal_zcy_char_csv_file(self, in_char_file):
        zcy_path = os.path.join(os.path.dirname(in_char_file), 'zcy_data')
        Common.check_path(zcy_path)
        # print('char_file: ', in_char_file)
        char_df = Common.read_csv_get_df(in_char_file)

        # print('self.x: ', self.x)
        # print(type(self.x))
        res_x1_values = Common.data_conversion(self.x, char_df['x'])
        lon = res_x1_values / (111000 * math.cos(self.lon / 180 * math.pi)) + self.lon
        lon = 2 * max(lon) - lon

        res_y1_values = Common.data_conversion(self.y, char_df['y'])
        lat = res_y1_values / 111000 + self.lat

        char_df['f_x'] = res_x1_values
        char_df['f_y'] = res_y1_values
        char_df['f_longitude'] = lon
        char_df['f_latitude'] = lat

        # 删除列
        columns_to_delete = ['map_width_pixel', 'map_height_pixel', 'map_width_cm', 'map_height_cm']
        res_char_data = char_df.drop(columns_to_delete, axis=1)

        # 生成zcy数据
        out_f = os.path.basename(in_char_file).split(".")[0] + f'_xyToLonLat_ZCY.csv'
        Common.df_write_to_csv(res_char_data, os.path.join(zcy_path, out_f))
        # 生成轨迹图
        Common.generate_images(res_x1_values, res_y1_values, lon, lat, zcy_path, '_走侧仪')

        # 筛选zcy数据
        print_with_line_number('筛选zcy数据')
        if 'direction' in res_char_data.columns:
            print_with_line_number('读取zcy direction数据')
            res_char_data = res_char_data[
                ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'direction', 'altitude']]
        else:
            res_char_data = res_char_data[
                ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'altitude']]

        return res_char_data

    def get_wifi_bluetooth_data(self):
        print_with_line_number('获取wifi 蓝牙数据')
        # 获取char文件
        in_char_file = self.config.get_char_file()
        # 获取wifi 蓝牙文件
        in_wifi_bluetooth_file = self.config.get_wifi_bluetooth_file()
        print_with_line_number(f'获取char文件：{in_char_file}')
        print_with_line_number(f'获取wifi bluetooth文件：{in_wifi_bluetooth_file}')
        # 处理char数据
        print_with_line_number('处理走测仪char文件')
        tmp_zcy_df = deal_data.deal_zcy_char_csv_file(in_char_file)
        # 处理wifi 蓝牙
        print_with_line_number('处理走测仪wifi bluetooth文件')
        tmp_wifi_bluetooth_df = deal_data.deal_wifi_bluetooth_csv_file(in_wifi_bluetooth_file)

        # 获取zcy，wifi 蓝牙合并数据
        tmp_merger_df = pd.merge(tmp_wifi_bluetooth_df, tmp_zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                 how='left')
        return tmp_merger_df

    def deal_wifi_bluetooth(self):
        print_with_line_number('----数据处理开始----')
        cur_path_output = os.path.join(os.path.dirname(self.config.get_wifi_bluetooth_file()), 'output')
        Common.check_path(cur_path_output)

        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
        elif 'finger'.lower() != self.config.get_data_type().lower():
            print_with_line_number(
                f'config.ini文件中，data_type字段填写错误,实际值：{self.config.get_data_type()}，期望值：uemr或者finger')
            return

        # 获取室内室外的数据
        print_with_line_number('----step 1:获取待处理数据----')
        n_scene = 'indoor'
        res_wifi_bluetooth_df = self.get_wifi_bluetooth_data()

        # 保存一份处理之前的数据(添加接口)
        cur_path_out_file = os.path.join(cur_path_output, f'wifi_bluetooth_{n_scene}_{data_type}_原始文件.csv')
        print_with_line_number(f'生成原始df数据文件：{cur_path_out_file}')
        Common.df_write_to_csv(res_wifi_bluetooth_df, cur_path_out_file)
        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        Common.set_scene_data(res_wifi_bluetooth_df, self.config)

        res_wifi_bluetooth_df = res_wifi_bluetooth_df.rename(
            columns={
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        # # 处理数据
        # net_type = zcy_ue_merge_df['Network Type'][0]
        # print_with_line_number(f'当前数据网络类型为：{net_type}')
        # print_with_line_number('----step 3:处理df数据----')
        # if 'LTE' == net_type:
        #     net_type = '4G'
        #     zcy_ue_merge_df = WalkTour.deal_4g_df_data(zcy_ue_merge_df, in_wifi_flag=True)
        # elif 'NR' == net_type:
        #     net_type = '5G'
        #     zcy_ue_merge_df = WalkTour.deal_5g_df_data(zcy_ue_merge_df, in_wifi_flag=True)

        # 生成输出文件名称
        print_with_line_number('----step 3:输出df数据为csv文件----')
        out_file = Common.wifi_bluetooth_generate_output_file_name(cur_path_output, res_wifi_bluetooth_df, n_scene,
                                                    'wifi_bluetooth')

        if 'uemr'.lower() == data_type:
            res_wifi_bluetooth_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            res_wifi_bluetooth_df = res_wifi_bluetooth_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        # out_file = r'D:\working\1214\1214国际财经中心(1)\国际财经中心\国际财经中心5G纵5_20231214\output\4G_HaiDian_indoor_WeTest_LOG_DT_UE_1214_finger'
        print_with_line_number(f'当前处理数据为：wifi_bluetooth {n_scene} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')

        Common.df_write_to_csv(res_wifi_bluetooth_df, out_file + f'_{data_type}.csv')

    def deal_walk_tour(self):
        # 处理walktour数据
        print_with_line_number('----开始处理walk_tour数据-----')

        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
        elif 'finger'.lower() != self.config.get_data_type().lower():
            print_with_line_number(f'config.ini文件中，data_type字段填写错误,实际值：{self.config.get_data_type()}，期望值：uemr或者finger')
            return

        cur_path_output = os.path.join(os.path.dirname(self.config.get_ue_file()), 'output')
        Common.check_path(cur_path_output)

        print_with_line_number(f'输出目录：{cur_path_output}')

        print_with_line_number('----step 1:获取原始df数据----')
        # 获取室内室外的数据
        if 'indoor'.lower() == self.config.get_test_area().lower():
            n_scene = 'indoor'
            print_with_line_number(f'获取 indoor 数据')
            zcy_ue_merge_df = WalkTour.indoor_get_df_zcy(self.config, self.deal_zcy_char_csv_file)
        elif 'outdoor'.lower() == self.config.get_test_area().lower():
            n_scene = 'outdoor'
            print_with_line_number(f'获取 outdoor 数据')
            zcy_ue_merge_df = WalkTour.outdoor_get_df(self.config)
        else:
            print_with_line_number(f'config.ini文件中，test_area字段填写错误,实际值：{self.config.get_test_area()}，期望值：indoor或者outdoor')
            return

        # 保存一份处理之前的数据(添加接口)
        cur_path_out_file = os.path.join(cur_path_output, f'WT_{n_scene}_{data_type}_原始文件.csv')
        print_with_line_number(f'生成原始df数据文件：{cur_path_out_file}')
        Common.df_write_to_csv(zcy_ue_merge_df, cur_path_out_file)
        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        zcy_ue_merge_df = Common.set_scene_data(zcy_ue_merge_df, self.config)

        # 处理数据
        net_type = zcy_ue_merge_df['Network Type'][0]
        print_with_line_number(f'当前数据网络类型为：{net_type}')
        # print('net_type: ', net_type)
        print_with_line_number('----step 3:处理df数据----')
        if 'LTE' == net_type:
            # print('室内 4G')
            net_type = '4G'
            zcy_ue_merge_df = WalkTour.deal_4g_df_data(zcy_ue_merge_df)
        elif 'NR' == net_type:
            net_type = '5G'
            zcy_ue_merge_df = WalkTour.deal_5g_df_data(zcy_ue_merge_df)

        # 生成输出文件名称
        out_file = Common.generate_output_file_name(cur_path_output, zcy_ue_merge_df, net_type, n_scene, 'WT')

        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
            zcy_ue_merge_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            zcy_ue_merge_df = zcy_ue_merge_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        print_with_line_number(f'当前处理数据为：WalkTour {net_type} {n_scene} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')
        Common.df_write_to_csv(zcy_ue_merge_df, out_file + f'_{data_type}.csv')

    def deal_wetest(self):
        print_with_line_number('----开始处理wetest数据----')

        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
        elif 'finger'.lower() != self.config.get_data_type().lower():
            print_with_line_number(f'config.ini文件中，data_type字段填写错误,实际值：{self.config.get_data_type()}，期望值：uemr或者finger')
            return

        cur_path_output = os.path.join(os.path.dirname(self.config.get_ue_file()), 'output')
        Common.check_path(cur_path_output)
        print_with_line_number(f'输出目录：{cur_path_output}')

        # 判断室内室外
        print_with_line_number('----step 1:获取原始df数据----')
        if 'indoor'.lower() == self.config.get_test_area().lower():
            # print('室内')
            n_scene = 'indoor'
            print_with_line_number(f'获取 indoor 数据')
            res_ue_df = WeTest.indoor_get_df(self.config, self.deal_zcy_char_csv_file)
        elif 'outdoor'.lower() == self.config.get_test_area().lower():
            n_scene = 'outdoor'
            print_with_line_number(f'获取 outdoor 数据')
            res_ue_df = WeTest.outdoor_get_df(self.config)
        else:
            print_with_line_number(f'config.ini文件中，test_area字段填写错误,实际值：{self.config.get_test_area()}，期望值：indoor或者outdoor')
            return

        # 保存一份处理之前的数据(添加接口)
        cur_path_out_file = os.path.join(cur_path_output, f'WeTest_{n_scene}_{data_type}_原始文件.csv')
        print_with_line_number(f'生成原始df数据文件：{cur_path_out_file}')
        Common.df_write_to_csv(res_ue_df, cur_path_out_file)
        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        res_ue_df = Common.set_scene_data(res_ue_df, self.config)

        # print('district: ', res_ue_df['f_district'][0])

        net_type = ''
        # 处理数据
        print_with_line_number('----step 3:处理df数据----')
        if 'lte_enb_id' in res_ue_df.columns:
            net_type = '4G'
            res_ue_df = WeTest.deal_wetest_4g(res_ue_df)
        elif 'nr_gnb_id' in res_ue_df.columns:
            net_type = '5G'
            res_ue_df = WeTest.deal_wetest_5g(res_ue_df)

        # 生成输出文件名称
        print_with_line_number('----step 4:输出df数据为csv文件----')
        out_file = Common.generate_output_file_name(cur_path_output, res_ue_df, net_type, n_scene, 'WeTest')

        if 'uemr'.lower() == data_type:
            res_ue_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            res_ue_df = res_ue_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        print_with_line_number(f'当前处理数据为：WeTest {net_type} {n_scene} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')
        Common.df_write_to_csv(res_ue_df, out_file + f'_{data_type}.csv')

    def deal_wifi_bluetooth_merge_ue(self):
        print_with_line_number('----开始处理walktour合并wifi、蓝牙数据-----')
        cur_path_output = os.path.join(os.path.dirname(self.config.get_ue_file()), 'output')
        Common.check_path(cur_path_output)

        data_type = 'finger'
        if 'uemr'.lower() == self.config.get_data_type().lower():
            data_type = 'uemr'
        elif 'finger'.lower() != self.config.get_data_type().lower():
            print_with_line_number(f'config.ini文件中，data_type字段填写错误,实际值：{self.config.get_data_type()}，期望值：uemr或者finger')
            return

        # 获取室内室外的数据
        # if '1' == self.config.get_scenario():
        print_with_line_number('----step 1:获取原始df数据----')
        n_scene = 'indoor'
        res_merge_df = WalkTour.indoor_get_df_wifi_bluetooth_zcy(self.config, self)
        # else:
        #     n_scene = 'outdoor'
        #     res_merge_df = WalkTour.outdoor_get_df(self.config)

        # 保存一份处理之前的数据(添加接口)
        cur_path_out_file = os.path.join(cur_path_output, f'WT_wifi_bluetooth_{n_scene}_{data_type}_原始文件.csv')
        print_with_line_number(f'生成原始df数据文件：{cur_path_out_file}')
        Common.df_write_to_csv(res_merge_df, cur_path_out_file)
        # 设置场景信息
        print_with_line_number('----step 2:设置场景信息----')
        zcy_ue_merge_df = Common.set_scene_data(res_merge_df, self.config)

        # 处理数据
        net_type = zcy_ue_merge_df['Network Type'][0]
        print_with_line_number(f'当前数据网络类型为：{net_type}')
        print_with_line_number('----step 3:处理df数据----')
        if 'LTE' == net_type:
            net_type = '4G'
            zcy_ue_merge_df = WalkTour.deal_4g_df_data(zcy_ue_merge_df, in_wifi_flag=True)
        elif 'NR' == net_type:
            net_type = '5G'
            zcy_ue_merge_df = WalkTour.deal_5g_df_data(zcy_ue_merge_df, in_wifi_flag=True)

        # 生成输出文件名称
        print_with_line_number('----step 4:输出df数据为csv文件----')
        out_file = Common.generate_output_file_name(cur_path_output, zcy_ue_merge_df, net_type, n_scene,
                                                    'WT_wifi_bluetooth')

        # data_type = 'finger'
        # if 'uemr'.lower() == self.config.get_data_type().lower():
        #     data_type = 'uemr'
        #     zcy_ue_merge_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
        #     zcy_ue_merge_df = zcy_ue_merge_df.rename(
        #         columns={
        #             'finger_id': 'uemr_id',
        #         })
        if 'uemr'.lower() == data_type:
            zcy_ue_merge_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
            res_ue_df = zcy_ue_merge_df.rename(
                columns={
                    'finger_id': 'uemr_id',
                })

        print_with_line_number(f'当前处理数据为：WT_wifi_bluetooth {net_type} {n_scene} {data_type} 数据')
        print_with_line_number(f'输出结果文件为：{out_file}' + f'_{data_type}.csv')

        Common.df_write_to_csv(zcy_ue_merge_df, out_file + f'_{data_type}.csv')


def print_with_line_number(message):
    # 获取当前行号
    current_line = inspect.currentframe().f_back.f_lineno
    # 使用 f-string 格式化字符串，包含文件名和行号信息
    print(f"{os.path.basename(__file__)}:{current_line} - {message}")


if __name__ == '__main__':
    circulate_flag = False
    while True:
        arguments = sys.argv
        # print('arguments: ', arguments)
        # print('len: ', len(arguments))
        if len(arguments) < 2 or circulate_flag:
            circulate_flag = False
            demo_config_file = input("请输入配置文件（使用绝对路径）: ").strip()
            while True:
                if not os.path.exists(demo_config_file):
                    demo_config_file = input("输入的配置文件不存在，请重新输入（使用绝对路径）: ")
                else:
                    break
        else:
            demo_config_file = arguments[1]
            if not os.path.exists(demo_config_file):
                print(f'输入的配置文件错误，当前输入为：{demo_config_file}')
                exit()

        config_project = Config(demo_config_file)
        deal_data = DealData(config_project)
        if 'WalkTour' == config_project.data_type:
            deal_data.deal_walk_tour()
        elif 'WeTest' == config_project.data_type:
            deal_data.deal_wetest()
        elif 'WIFI_BlueTooth' == config_project.data_type:
            if Common.check_file_exists(config_project.get_table_file()) and Common.check_file_exists(config_project.get_ue_file()):
                print_with_line_number('----处理wifi 蓝牙和 test log数据----')
                deal_data.deal_wifi_bluetooth_merge_ue()
            else:
                print_with_line_number('----只处理wifi 蓝牙数据----')
                deal_data.deal_wifi_bluetooth()
        else:
            print_with_line_number(config_project.data_type)
        out_flag = input("按回车键继续，输入out退出 ")
        if 'out' == out_flag.lower():
            break
        else:
            circulate_flag = True

