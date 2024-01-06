# -*- coding: utf-8 -*-
import os.path

from Common import *
from DealData import DealData


class WalkTour:
    class Indoor:
        @staticmethod
        def deal_LTE():
            # 读取处理走测仪数据
            zcy_df = read_csv_get_df(zcy_file)
            table_df = read_csv_get_df(table_file)
            in_ue_df = read_csv_get_df(ue_file)

            ue_merge_df = DealData.walktour_merge_ue_imei(in_ue_df, table_df)
            ue_df = DealData.walktour_indoor_merge_ue_zcy(ue_merge_df, zcy_df)
            ue_df = DealData.deal_WalkTour_indoor_4g(ue_df, f_msisdn, set_scene_data)
            df_write_to_csv(ue_df, out_file)

        @staticmethod
        def deal_NR():
            zcy_df = read_csv_get_df(zcy_file)
            in_ue_df = read_csv_get_df(ue_file)
            table_df = read_csv_get_df(table_file)

            ue_merge_df = DealData.walktour_merge_ue_imei(in_ue_df, table_df)
            in_ue_df = DealData.walktour_indoor_merge_ue_zcy(ue_merge_df, zcy_df)
            in_ue_df = DealData.deal_WalkTour_indoor_5g(in_ue_df, f_msisdn, set_scene_data)
            df_write_to_csv(in_ue_df, out_file)

    class Outdoor:
        @staticmethod
        def deal_LTE():
            ue_df = read_csv_get_df(ue_file)
            imei_df = read_csv_get_df(table_file)
            ue_merge_df = DealData.walktour_merge_ue_imei(ue_df, imei_df)
            ue_df = DealData.deal_WalkTour_outdoor_4g(ue_merge_df, f_msisdn, set_scene_data)
            df_write_to_csv(ue_df, out_file)

        @staticmethod
        def deal_NR():
            ue_df = read_csv_get_df(ue_file)
            imei_df = read_csv_get_df(table_file)
            ue_merge_df = DealData.walktour_merge_ue_imei(ue_df, imei_df)
            ue_df = DealData.deal_WalkTour_outdoor_5g(ue_merge_df, f_msisdn, set_scene_data)
            df_write_to_csv(ue_df, out_file)


def set_scene_data(log_df):
    # 设置场景信息
    log_df['f_device_brand'] = f_device_brand
    log_df['f_device_model'] = f_device_model
    log_df['f_area'] = f_area
    log_df['f_floor'] = f_floor
    log_df['f_scenario'] = f_scenario
    log_df['f_province'] = "北京"
    log_df['f_city'] = "北京"
    log_df['f_district'] = f_district
    log_df['f_street'] = f_street
    log_df['f_building'] = f_building
    log_df['f_prru_id'] = 0
    log_df['f_source'] = f_source
    return log_df


f_msisdn_dict = {'2934': '533F8040D9351F4A9499FC7825805B14',
                 '8539': '7314E1BE6DF72134E285D6AC1A99D8B7'}

if __name__ == '__main__':
    # 填充测试数据的场景等信息_华为室内：
    f_floor = '4F'
    # f_area = 'A1值机岛'
    f_device_brand = "HUAWEI"
    f_device_model = "P40"
    f_scenario = 1
    f_district = '大兴区'
    f_street = '大兴国际机场'
    f_building = '航站楼'
    f_source = '测试log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙

    # f_msisdn = '533F8040D9351F4A9499FC7825805B14'  # 2934
    # f_msisdn = '7314E1BE6DF72134E285D6AC1A99D8B7'  # 8539

    # 公共设置
    # data_path = r'E:\work\mr_dea_data_c1\test_data\12月4号\G1\NR\8539'
    data_path = get_file_data()
    print('data_path: ', data_path)
    out_path = os.path.join(data_path, 'output')
    check_path(out_path)

    # feature_str = get_dir_base_name(os.path.dirname(data_path))
    feature_str = get_dir_base_name(data_path)
    print('feature_str: ', feature_str)

    ue_file = get_file_by_string('UE', data_path)
    table_file = get_file_by_string('table', data_path)
    zcy_file = get_file_by_string('ZCY', data_path)

    imei_v = zcy_file.split('-')[4].split('_')[0]
    print('imei_v: ', imei_v)
    print('type: ', type(imei_v))
    f_msisdn = f_msisdn_dict[int(imei_v)]
    print('f_msisdn: ', type(f_msisdn))

    print('ue_file: ', os.path.basename(ue_file))
    f_area = zcy_file.split('-')[1] + '值机岛'
    print('f_area: ', f_area)
    print('table_file: ', os.path.basename(table_file))
    print('zcy_file: ', zcy_file)
    # 根据输入文件生成输出文件
    index = find_nth_occurrence(os.path.basename(zcy_file), '-', 4)
    tmp_v_f = os.path.basename(zcy_file)[:index].replace('-', '_') + '_'
    tmp_v_f = tmp_v_f.replace(' ', '_')
    file_name = 'Merge_{}_WT_LOG_DT_{}_UE_{}'.format(tmp_v_f, formatted_date, feature_str)
    out_file = os.path.join(out_path, file_name + '.csv')
    print('out_file: ', out_file)

    # 把out写入到文件中，merge中使用
    data_add_to_file(out_file)
    data_add_to_file(file_name)

    if 'LTE' in data_path:
        WalkTour.Indoor.deal_LTE()
    elif 'NR' in data_path:
        WalkTour.Indoor.deal_NR()
    # WalkTour.Outdoor.deal_LTE()
    # WalkTour.Outdoor.deal_NR()
