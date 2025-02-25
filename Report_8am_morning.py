import calendar
import sys
import textwrap
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Connection, Engine

from database_op import DatabaseOp
from utils import CommonUtils


class Report8AmMorning(object):
    database_op: DatabaseOp = DatabaseOp()
    common_utils: CommonUtils = CommonUtils()

    def __init__(self, config_path: str, report_date: str, time_flag: str) -> None:
        self.config_path = config_path
        self.report_date = report_date
        self.time_flag = time_flag

    def get_config(self) -> Dict[str, pd.DataFrame]:
        """
        获取配置信息
        :return:
        """
        config_dfs: Dict = pd.read_excel(self.config_path, sheet_name=None)

        return config_dfs

    def get_newhouse_daily_deal_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取新房每日成交数据
        :param start_date:
        :param end_date:
        :return:
        """
        conn_url: str = self.database_op.get_db_conn_url('academe_dataspider')
        house_test_conn_url: str = self.database_op.get_db_conn_url('house_test')
        dataspider_conn: Connection = self.database_op.get_db_connection(conn_url)
        house_test_conn: Connection = self.database_op.get_db_connection(house_test_conn_url)
        newhouse_config_df: pd.DataFrame = self.get_config()['新房配置']
        # 获取物业类型为"商品住宅"+数据源为"官方发布"的城市列表
        proptype_house_cities: Tuple[str] = tuple(newhouse_config_df['城市']
                                                  [(newhouse_config_df['物业类型'] == '商品住宅')
                                                   & (newhouse_config_df['数据来源'] == '官方发布')].tolist())
        # 获取物业类型为"商品房"+数据源为"官方发布"的城市列表
        proptype_all_cities: Tuple[str] | str = tuple(newhouse_config_df['城市']
                                                      [(newhouse_config_df['物业类型'] == '商品房')
                                                       & (newhouse_config_df['数据来源'] == '官方发布')].tolist())
        # 获取物业类型为"商品住宅"+数据源为"项目汇总"的城市列表
        project_summary_cities: Tuple[str] | str = tuple(newhouse_config_df['城市']
                                                         [(newhouse_config_df['物业类型'] == '商品住宅')
                                                          & (newhouse_config_df['数据来源'] == '项目汇总')].tolist())
        # 处理单个城市情况
        if len(proptype_all_cities) == 1:
            proptype_all_cities = f"('{proptype_all_cities[0]}')"
        if len(project_summary_cities) == 1:
            project_summary_cities = f"('{project_summary_cities[0]}')"

        sql = f"""
                SELECT city_name, data_date, chengjiao_area
                FROM Academe_DataSpider.dbo.CRED_Macro_Deal_Data
                WHERE index_name1 = '商品住宅'
                  AND index_name2 = '总体'
                  AND date_type = 'day'
                  AND dimension_type = '城市'
                  AND city_name IN {proptype_house_cities}
                  AND data_date BETWEEN '{start_date}' AND '{end_date}'
                UNION
                SELECT city_name, data_date, chengjiao_area
                FROM Academe_DataSpider.dbo.CRED_Macro_Deal_Data
                WHERE index_name1 = '商品房'
                  AND index_name2 = '总体'
                  AND date_type = 'day'
                  AND dimension_type = '城市'
                  AND city_name IN {proptype_all_cities}
                  AND data_date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY city_name, data_date;
                """
        sql_test = f"""
                    SELECT dimension_value, data_date, deal_area
                    FROM dbo.temp_cred_macro_deal_data
                    WHERE property_type = '商品住宅'
                      AND index_name = '总体'
                      AND date_type = 'day'
                      AND dimension_type = '城市'
                      AND dimension_value IN {proptype_house_cities}
                      AND data_date BETWEEN '{start_date}' AND '{end_date}'
                    UNION
                    SELECT dimension_value, data_date, deal_area
                    FROM dbo.temp_cred_macro_deal_data
                    WHERE property_type = '商品房'
                      AND index_name = '总体'
                      AND date_type = 'day'
                      AND dimension_type = '城市'
                      AND dimension_value IN {proptype_all_cities}
                      AND data_date BETWEEN '{start_date}' AND '{end_date}'
                    ORDER BY dimension_value, data_date;
        """
        sql_project_summary = f"""
                SELECT dimension_value, data_date, deal_area
                FROM dbo.temp_CRED_LowDealData
                WHERE property_type = '商品住宅'
                  AND date_type = 'day'
                  AND dimension_type = '城市'
                  AND dimension_value IN {project_summary_cities}
                  AND data_date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY dimension_value, data_date;
                """
        sql_chengdu = f"""
                WITH HouseDealData AS (SELECT district,
                                              type,
                                              data_time,
                                              zz_area,
                                              create_time,
                                              ROW_NUMBER() OVER (PARTITION BY district, type, data_time ORDER BY create_time DESC) rn
                                       FROM Academe_Business.dbo.cih_macro_chengdu_today_deal
                                       WHERE district = '全市'
                                         AND type = 'spf'
                                         AND CONVERT(DATE, data_time) BETWEEN '{start_date}' AND '{end_date}')
                SELECT '成都' AS                                      city_name,
                       CONVERT(DATE, data_time)                       data_date,
                       ROUND(SUM(CONVERT(FLOAT, zz_area)) / 10000, 2) house_area
                FROM HouseDealData
                WHERE rn = 1
                GROUP BY CONVERT(DATE, data_time)
                ORDER BY city_name, data_date ;
        """
        column_list: List[str] = ['城市', '数据日期', '成交面积']
        data_df: pd.DataFrame = pd.read_sql(sql, dataspider_conn)
        test_data_df: pd.DataFrame = pd.read_sql(sql_test, house_test_conn)
        data_df.columns = column_list
        test_data_df.columns = column_list
        data_df['成交面积'] = data_df['成交面积'].apply(lambda x: x / 10000 if pd.notnull(x) else x)
        test_data_df['成交面积'] = test_data_df['成交面积'].apply(lambda x: x / 10000 if pd.notnull(x) else x)
        project_summary_data_df: pd.DataFrame = pd.read_sql(sql_project_summary, house_test_conn)

        # 成都新房数据临时从本地获取
        # chengdu_newhouse_deal_df: pd.DataFrame = pd.read_excel(r'data_files/成都商品房日度成交数据.xlsx')
        # chengdu_newhouse_deal_df['数据日期'] = pd.to_datetime(chengdu_newhouse_deal_df['数据日期'])
        # # 筛选指定日期范围内的成交数据
        # chengdu_newhouse_deal_df = chengdu_newhouse_deal_df[(chengdu_newhouse_deal_df['数据日期'] >= start_date)
        #                                                    & (chengdu_newhouse_deal_df['数据日期'] <= end_date)]
        # 成都新房数据临时从采集表获取
        chengdu_newhouse_deal_df: pd.DataFrame = pd.read_sql(sql_chengdu, dataspider_conn)
        chengdu_newhouse_deal_df.columns = column_list

        project_summary_data_df.columns = column_list
        dfs_to_concat = []
        for df in [data_df, project_summary_data_df, chengdu_newhouse_deal_df, test_data_df]:
            if not df.empty:
                dfs_to_concat.append(df)
        data_df = pd.concat(dfs_to_concat, ignore_index=True)
        data_df.drop_duplicates(subset=['城市', '数据日期'], inplace=True)
        data_df['数据日期'] = pd.to_datetime(data_df['数据日期'])
        # 按城市、数据日期排序
        data_df = data_df.sort_values(by=['城市', '数据日期'], ascending=[True, True])

        return data_df

    def get_secondhouse_daily_deal_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取二手房每日成交数据
        :param start_date:
        :param end_date:
        :return:
        """
        conn_url: str = self.database_op.get_db_conn_url('academe_dataspider')
        house_test_conn: Connection = self.database_op.get_db_conn_url('house_test')
        dataspider_conn: Connection = self.database_op.get_db_connection(conn_url)
        secondhouse_config_df: pd.DataFrame = self.get_config()['二手房配置']
        # 获取物业类型为"二手商品住宅"的城市列表
        proptype_house_cities: Tuple[str] = tuple(secondhouse_config_df['城市']
                                                  [secondhouse_config_df['物业类型'] == '二手商品住宅'].tolist())
        # 获取物业类型为"二手商品房"的城市列表
        proptype_all_cities: Tuple[str] | str = tuple(secondhouse_config_df['城市']
                                                      [secondhouse_config_df['物业类型'] == '二手商品房'].tolist())
        # 处理单个城市情况
        if len(proptype_all_cities) == 1:
            proptype_all_cities = f"('{proptype_all_cities[0]}')"
        sql = f"""  
                SELECT city_name, data_date, chengjiao_set
                FROM Academe_DataSpider.dbo.CRED_Macro_Deal_Data
                WHERE index_name1 = '二手商品住宅'
                  AND date_type = 'day'
                  AND dimension_type = '城市'
                  AND city_name IN {proptype_house_cities}
                  AND data_date BETWEEN '{start_date}' AND '{end_date}'
                UNION
                SELECT city_name, data_date, chengjiao_set
                FROM Academe_DataSpider.dbo.CRED_Macro_Deal_Data
                WHERE index_name1 = '二手商品房'
                  AND date_type = 'day'
                  AND dimension_type = '城市'
                  AND city_name IN {proptype_all_cities}
                  AND data_date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY city_name, data_date;
                """
        sql_test = f"""
                    SELECT dimension_value, data_date, deal_num
                    FROM dbo.temp_cred_macro_deal_data
                    WHERE property_type = '二手商品住宅'
                      AND date_type = 'day'
                      AND dimension_type = '城市'
                      AND dimension_value IN {proptype_house_cities}
                      AND data_date BETWEEN '{start_date}' AND '{end_date}'
                    UNION
                    SELECT dimension_value, data_date, deal_num
                    FROM dbo.temp_cred_macro_deal_data
                    WHERE property_type = '二手商品房'
                      AND date_type = 'day'
                      AND dimension_type = '城市'
                      AND dimension_value IN {proptype_all_cities}
                      AND data_date BETWEEN '{start_date}' AND '{end_date}'
                    ORDER BY dimension_value, data_date;
                """
        data_df: pd.DataFrame = pd.read_sql(sql, dataspider_conn)
        test_data_df: pd.DataFrame = pd.read_sql(sql_test, house_test_conn)
        column_list: List[str] = ['城市', '数据日期', '成交套数']
        data_df.columns = column_list
        test_data_df.columns = column_list
        dfs_to_concat = []
        for df in [data_df, test_data_df]:
            if not df.empty:
                dfs_to_concat.append(df) 
        data_df = pd.concat(dfs_to_concat, ignore_index=True)
        data_df.drop_duplicates(subset=['城市', '数据日期'], inplace=True)
        data_df['数据日期'] = pd.to_datetime(data_df['数据日期'])
        # 按城市、数据日期排序
        data_df = data_df.sort_values(by=['城市', '数据日期'], ascending=[True, True])

        return data_df

    def get_newhouse_available_data(self, end_date: str) -> pd.DataFrame:
        """
        获取新房可售数据
        :param end_date:
        :return:
        """
        # end_date = self.date_utils.get_data_date_interval(self.report_date)[1]
        conn_url: str = self.database_op.get_db_conn_url('academe_dataspider')
        dataspider_conn: Connection = self.database_op.get_db_connection(conn_url)
        newhouse_config_df: pd.DataFrame = self.get_config()['新房可售配置']
        # 获取需要统计可售数据的城市列表
        city_list: Tuple[str] = tuple(newhouse_config_df['城市'].tolist())

        sql = f"""
                SELECT city_name, keshou_set, keshou_area
                FROM Academe_DataSpider.dbo.CRED_Macro_Deal_Data
                WHERE index_name1 = '商品住宅'
                  AND index_name2 = '总体'
                  AND date_type = 'day'
                  AND dimension_type = '城市'
                  AND city_name IN {city_list}
                  AND data_date = '{end_date}'
                ORDER BY city_name;
                """
        data_df: pd.DataFrame = pd.read_sql(sql, dataspider_conn)
        column_list: List[str] = ['城市', '可售套数', '可售面积']
        data_df.columns = column_list
        data_df['可售套数'] = data_df['可售套数'].astype(int)
        data_df['可售面积'] = data_df['可售面积'].apply(lambda x: round(x / 10000, 2))

        return data_df

    def data_statistics(self) -> pd.DataFrame:
        """
        数据统计：分周月度
        :return:
        """
        newhouse_deal_city_list: List[str] = self.get_config()['新房配置']['城市'].tolist()
        secondhouse_deal_city_list: List[str] = self.get_config()['二手房配置']['城市'].tolist()
        newhouse_available_city_list: List[str] = self.get_config()['新房可售配置']['城市'].tolist()
        # 新房相关数据统计
        if self.time_flag == 'w':    # 周度数据统计
            # 新房本年度交易数据统计
            current_start_date, current_end_date = self.common_utils.get_data_date_interval(
                specified_date=self.report_date, delta_days=55)
            current_week_satuaday: pd.Timestamp = (pd.to_datetime(self.report_date) - pd.Timedelta(days=1))
            current_month_first_day: str = current_week_satuaday.replace(day=1).strftime('%Y-%m-%d')
            current_week_satuaday: str = current_week_satuaday.strftime('%Y-%m-%d')
            days: int = pd.to_datetime(current_week_satuaday).day_of_year
            last_month_first_day: str = (pd.to_datetime(current_month_first_day) -
                                         pd.Timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
            last_month_end_date: str = (pd.to_datetime(current_month_first_day) -
                                        pd.Timedelta(days=1)).replace(day=days).strftime('%Y-%m-%d')
            if pd.to_datetime(current_start_date) > pd.to_datetime(last_month_first_day):
                current_start_date = last_month_first_day

            current_year_newhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                newhouse_deal_city_list, current_start_date, current_end_date)
            db_current_year_newhouse_deal_df: pd.DataFrame = self.get_newhouse_daily_deal_data(current_start_date,
                                                                                               current_end_date)
            current_year_newhouse_deal_df = current_year_newhouse_deal_df.merge(db_current_year_newhouse_deal_df,
                                                                              on=['城市', '数据日期'], how='left')
            current_year_newhouse_deal_df.drop_duplicates(inplace=True)

            # 新房去年交易数据统计
            last_year_same_week_sunday: str = self.common_utils.get_last_year_week_date(self.report_date)
            last_year_same_week_start_date, last_year_same_week_end_date = self.common_utils.get_data_date_interval(
                specified_date=last_year_same_week_sunday, delta_days=6)
            last_year_same_week_satuaday: pd.Timestamp = (pd.to_datetime(last_year_same_week_sunday)
                                                          - pd.Timedelta(days=1))
            last_year_same_month_first_day: str = last_year_same_week_satuaday.replace(day=1).strftime('%Y-%m-%d')
            last_year_same_month_last_day: str = last_year_same_week_satuaday.replace(day=days).strftime('%Y-%m-%d')
            last_year_start_date: str | None = None
            last_year_end_date: str | None = None
            if pd.to_datetime(last_year_same_week_start_date) > pd.to_datetime(last_year_same_month_first_day):
                last_year_start_date = last_year_same_month_first_day
            else:
                last_year_start_date = last_year_same_week_start_date
            if pd.to_datetime(last_year_same_week_end_date) > pd.to_datetime(last_year_same_month_last_day):
                last_year_end_date = last_year_same_week_end_date
            else:
                last_year_end_date = last_year_same_month_last_day

            last_year_newhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                newhouse_deal_city_list, last_year_start_date, last_year_end_date)
            db_last_year__newhouse_deal_df: pd.DataFrame = self.get_newhouse_daily_deal_data(
                last_year_start_date, last_year_end_date)
            last_year_newhouse_deal_df = last_year_newhouse_deal_df.merge(
                db_last_year__newhouse_deal_df, on=['城市', '数据日期'], how='left')

            # 周度可售：统计本周期周六的可售套数、面积（万㎡）。若周六数据缺失且无法补充，则可用最近日期的可售代替周度可售数据。
            current_week_newhouse_available_df: pd.DataFrame = self.get_newhouse_available_data(current_week_satuaday)

            # 周度可售：统计上周周六的可售套数、面积（万㎡）。若周六数据缺失且无法补充，则可用最近日期的可售代替周度可售数据。
            last_week_satuaday: str = (pd.to_datetime(current_week_satuaday)
                                       - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
            last_week_newhouse_available_df: pd.DataFrame = self.get_newhouse_available_data(last_week_satuaday)

            # 二手房相关数据统计
            # 二手房本年度交易数据统计
            current_year_secondhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                secondhouse_deal_city_list, current_start_date, current_end_date)
            db_current_year_secondhouse_deal_df: pd.DataFrame = self.get_secondhouse_daily_deal_data(current_start_date,
                                                                                                    current_end_date)
            current_year_secondhouse_deal_df = current_year_secondhouse_deal_df.merge(
                db_current_year_secondhouse_deal_df, on=['城市', '数据日期'], how='left')
            current_year_secondhouse_deal_df.drop_duplicates(inplace=True)

            # 二手房去年交易数据统计
            last_year_secondhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                secondhouse_deal_city_list, last_year_start_date, last_year_end_date)
            db_last_year_secondhouse_deal_df: pd.DataFrame = self.get_secondhouse_daily_deal_data(
                last_year_start_date, last_year_end_date)
            last_year_secondhouse_deal_df = last_year_secondhouse_deal_df.merge(
                db_last_year_secondhouse_deal_df, on=['城市', '数据日期'], how='left')
            last_year_secondhouse_deal_df.drop_duplicates(inplace=True)

            # 衢州新房成交缺数处理：节假日或周末缺数默认补0
            current_year: int = pd.to_datetime(current_end_date).year
            holiday_dates: List[datetime.date] = self.common_utils.get_cn_holidays([current_year - 1, current_year])
            current_year_newhouse_deal_df.loc[((current_year_newhouse_deal_df['城市'] == '衢州')
                                               & (current_year_newhouse_deal_df['成交面积'].isnull())), '成交面积'] \
                = (current_year_newhouse_deal_df.loc[((current_year_newhouse_deal_df['城市'] == '衢州')
                                               & (current_year_newhouse_deal_df['成交面积'].isnull())), :]
                   .apply(lambda x: 0 if x['数据日期'].date() in holiday_dates
                                         or x['数据日期'].weekday() in (5, 6) else x['成交面积'], axis=1))
            last_year_newhouse_deal_df.loc[((last_year_newhouse_deal_df['城市'] == '衢州')
                                            & (last_year_newhouse_deal_df['成交面积'].isnull())), '成交面积'] \
                = (last_year_newhouse_deal_df.loc[((last_year_newhouse_deal_df['城市'] == '衢州')
                                                   & (last_year_newhouse_deal_df['成交面积'].isnull())), :]
                   .apply(lambda x: 0 if x['数据日期'].date() in holiday_dates
                                         or x['数据日期'].weekday() in (5, 6) else x['成交面积'], axis=1))

            # 存储上述统计结果，用于补数确认
            with pd.ExcelWriter(f'data_files/报告原始日度数据-周度.xlsx') as writer:
                current_year_newhouse_deal_df.to_excel(writer, sheet_name='新房-本年度交易', index=False)
                last_year_newhouse_deal_df.to_excel(writer, sheet_name='新房-去年交易', index=False)
                current_week_newhouse_available_df.to_excel(writer, sheet_name='新房-本周可售', index=False)
                last_week_newhouse_available_df.to_excel(writer, sheet_name='新房-上周可售', index=False)
                current_year_secondhouse_deal_df.to_excel(writer, sheet_name='二手房-本年度交易', index=False)
                last_year_secondhouse_deal_df.to_excel(writer, sheet_name='二手房-去年交易', index=False)

            # 缺数天数统计
            current_year_newhouse_deal_df['缺数'] = (current_year_newhouse_deal_df['成交面积']
                                                     .apply(lambda x: 1 if pd.isnull(x) else 0))
            last_year_newhouse_deal_df['缺数'] = (last_year_newhouse_deal_df['成交面积']
                                                  .apply(lambda x: 1 if pd.isnull(x) else 0))
            current_year_secondhouse_deal_df['缺数'] = (current_year_secondhouse_deal_df['成交套数']
                                                       .apply(lambda x: 1 if pd.isnull(x) else 0))
            last_year_secondhouse_deal_df['缺数'] = (last_year_secondhouse_deal_df['成交套数']
                                                    .apply(lambda x: 1 if pd.isnull(x) else 0))
            current_week_newhouse_available_shortage_city_list: List[str] = list(
                set(newhouse_available_city_list).difference(set(current_week_newhouse_available_df['城市'].tolist())))
            last_week_newhouse_available_shortage_city_list: List[str] = list(
                set(newhouse_available_city_list).difference(set(last_week_newhouse_available_df['城市'].tolist())))
            print('本年度新房缺数情况：')
            print(current_year_newhouse_deal_df.groupby('城市')['缺数'].sum())
            print('去年同期新房缺数情况：')
            print(last_year_newhouse_deal_df.groupby('城市')['缺数'].sum())
            print('本年度二手房缺数情况：')
            print(current_year_secondhouse_deal_df.groupby('城市')['缺数'].sum())
            print('去年同期二手房缺数情况：')
            print(last_year_secondhouse_deal_df.groupby('城市')['缺数'].sum())
            print(f'本周新房可售缺数城市：{current_week_newhouse_available_shortage_city_list}')
            print(f'上周新房可售缺数城市：{last_week_newhouse_available_shortage_city_list}')


            prompt_msg: str = f"""
            原始数据已存储至【data_files/报告原始日度数据-周度.xlsx】。
            请确认以上数据是否正确，若数据有缺失，请确认补数方法：
            1--人工手动补数；2--程序自动补数（待开发）。
            注意，人工手动补数完成后请保存并关闭文件；若无需补数，请回车继续。
            """
            prompt_input: str = input(textwrap.dedent(prompt_msg))
            if prompt_input == '1':
                # 手动补数，重新加载补数后的文件
                data_dfs: Dict[str, pd.DataFrame] = pd.read_excel(f'data_files/报告原始日度数据-周度.xlsx',
                                                                  sheet_name=None)
                current_year_newhouse_deal_df = data_dfs['新房-本年度交易']
                last_year_newhouse_deal_df = data_dfs['新房-去年交易']
                current_week_newhouse_available_df = data_dfs['新房-本周可售']
                last_week_newhouse_available_df = data_dfs['新房-上周可售']
                current_year_secondhouse_deal_df = data_dfs['二手房-本年度交易']
                last_year_secondhouse_deal_df = data_dfs['二手房-去年交易']
            elif prompt_input == '2':
                # 自动补数，待开发
                sys.exit()
            else:
                pass

            # 为上述统计数据增加"梯队"和"周度数"
            city_level_df: pd.DataFrame = self.get_config()['城市梯队']
            city_level_dict: Dict[str, str] = city_level_df.set_index('城市')['梯队'].to_dict()
            current_year_newhouse_deal_df['梯队'] = current_year_newhouse_deal_df['城市'].map(city_level_dict)
            current_year_newhouse_deal_df['周度数'] = current_year_newhouse_deal_df['数据日期'].apply(
                lambda x: self.common_utils.get_year_week(x))
            last_year_newhouse_deal_df['梯队'] = last_year_newhouse_deal_df['城市'].map(city_level_dict)
            last_year_newhouse_deal_df['周度数'] = last_year_newhouse_deal_df['数据日期'].apply(
                lambda x: self.common_utils.get_year_week(x))
            current_week_newhouse_available_df['梯队'] = current_week_newhouse_available_df['城市'].map(city_level_dict)
            last_week_newhouse_available_df['梯队'] = last_week_newhouse_available_df['城市'].map(city_level_dict)
            current_year_secondhouse_deal_df['梯队'] = current_year_secondhouse_deal_df['城市'].map(city_level_dict)
            current_year_secondhouse_deal_df['周度数'] = current_year_secondhouse_deal_df['数据日期'].apply(
                lambda x: self.common_utils.get_year_week(x))
            last_year_secondhouse_deal_df['梯队'] = last_year_secondhouse_deal_df['城市'].map(city_level_dict)
            last_year_secondhouse_deal_df['周度数'] = last_year_secondhouse_deal_df['数据日期'].apply(
                lambda x: self.common_utils.get_year_week(x))

            recent8_week_nums: List[str] = []
            for i in range(8):
                previous_week_saturday = pd.to_datetime(current_week_satuaday) - timedelta(weeks=i)
                week_number = self.common_utils.get_year_week(previous_week_saturday)
                recent8_week_nums.append(week_number)
            recent8_week_nums.reverse()
            sort_order = recent8_week_nums
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)


            # 周度交易：统计近8周的交易面积（万㎡），即截至本周六共56天。
            recent8week_newhouse_deal_df: pd.DataFrame = current_year_newhouse_deal_df[
                current_year_newhouse_deal_df['数据日期'] >= current_start_date
            ]
            # recent8week_newhouse_deal_df['周度数'] = recent8week_newhouse_deal_df['周度数'].astype(category_type)
            # recent8week_newhouse_deal_df = recent8week_newhouse_deal_df.sort_values(by='周度数')

            # 月度交易：统计从本月1号起截至本周六的交易面积（万㎡）。
            current_month_newhouse_deal_df: pd.DataFrame = current_year_newhouse_deal_df[
                (current_year_newhouse_deal_df['数据日期'] >= current_month_first_day)
                & (current_year_newhouse_deal_df['数据日期'] <= current_week_satuaday)]

            # 月度交易：统计从上月相同周期的交易面积（万㎡），用于计算环比。比如本月：2024.10.1-10.27，则上月：2024.09.1-10.27。
            last_month_newhouse_deal_df: pd.DataFrame = current_year_newhouse_deal_df[
                (current_year_newhouse_deal_df['数据日期'] >= last_month_first_day)
                & (current_year_newhouse_deal_df['数据日期'] <= last_month_end_date)]

            # 月度交易：统计去年同月份相同周期的交易面积（万㎡），用于计算月度同比。比如本月：2024.10.1-10.27，则去年同月：2023.10.1-10.27。
            last_year_same_month_newhouse_deal_df: pd.DataFrame = last_year_newhouse_deal_df[
                (last_year_newhouse_deal_df['数据日期'] >= last_year_same_month_first_day)
                & (last_year_newhouse_deal_df['数据日期'] <= last_year_same_month_last_day)
            ]

            # 周度交易：统计去年同周度的交易面积（万㎡），用于计算周度同比。
            last_year_same_week_newhouse_deal_df: pd.DataFrame = last_year_newhouse_deal_df[
                (last_year_newhouse_deal_df['数据日期'] >= last_year_same_week_start_date)
                & (last_year_newhouse_deal_df['数据日期'] <= last_year_same_week_end_date)]

            # 二手房相关数据统计
            # 周度交易：统计近8周的交易套数，即截至本周六共56天。
            recent8week_secondhouse_deal_df: pd.DataFrame = current_year_secondhouse_deal_df[
                current_year_secondhouse_deal_df['数据日期'] >= current_start_date
            ]
            # recent8week_secondhouse_deal_df['周度数'] = recent8week_secondhouse_deal_df['周度数'].astype(category_type)
            # recent8week_secondhouse_deal_df = recent8week_secondhouse_deal_df.sort_values(by='周度数')

            # 月度交易：统计从本月1号起截至本周六的交易套数。
            current_month_secondhouse_deal_df: pd.DataFrame = current_year_secondhouse_deal_df[
                (current_year_secondhouse_deal_df['数据日期'] >= current_month_first_day)
                & (current_year_secondhouse_deal_df['数据日期'] <= current_week_satuaday)]

            # 月度交易：统计从上月相同周期的交易套数，用于计算环比。比如本月：2024.10.1-10.27，则上月：2024.09.1-10.27。
            last_month_secondhouse_deal_df: pd.DataFrame = current_year_secondhouse_deal_df[
                (current_year_secondhouse_deal_df['数据日期'] >= last_month_first_day)
                & (current_year_secondhouse_deal_df['数据日期'] <= last_month_end_date)]

            # 月度交易：统计去年同月份相同周期的成交套数，用于计算月度同比。比如本月：2024.10.1-10.27，则去年同月：2023.10.1-10.27。
            last_year_same_month_secondhouse_deal_df: pd.DataFrame = last_year_secondhouse_deal_df[
                (last_year_secondhouse_deal_df['数据日期'] >= last_year_same_month_first_day)
                & (last_year_secondhouse_deal_df['数据日期'] <= last_year_same_month_last_day)
            ]

            # 周度交易：统计去年同周度的交易套数，用于计算周度同比。
            last_year_same_week_secondhouse_deal_df: pd.DataFrame = last_year_secondhouse_deal_df[
                (last_year_secondhouse_deal_df['数据日期'] >= last_year_same_week_start_date)
                & (last_year_secondhouse_deal_df['数据日期'] <= last_year_same_week_end_date)]

            # 按周度数分组统计近8周新房的交易面积（万㎡）、二手房的交易套数，即截至本周六共56天。
            recent8week_newhouse_deal_byweek_df = (recent8week_newhouse_deal_df
                                             .groupby('周度数')['成交面积'].sum()).reset_index()
            recent8week_newhouse_deal_byweek_df.columns = ['周度数', '成交面积']
            recent8week_secondhouse_deal_byweek_df = (recent8week_secondhouse_deal_df
                                               .groupby('周度数')['成交套数'].sum()).reset_index()
            recent8week_secondhouse_deal_byweek_df.columns = ['周度数', '成交套数']
            recent8week_newhouse_deal_byweek_df['周度数'] = (recent8week_newhouse_deal_byweek_df['周度数']
                                                             .astype(category_type))
            recent8week_newhouse_deal_byweek_df = recent8week_newhouse_deal_byweek_df.sort_values(by='周度数')
            recent8week_secondhouse_deal_byweek_df['周度数'] = (recent8week_secondhouse_deal_byweek_df['周度数']
                                                                .astype(category_type))
            recent8week_secondhouse_deal_byweek_df = recent8week_secondhouse_deal_byweek_df.sort_values(by='周度数')

            # 按梯队、周度数分组统计近4周新房的交易面积（万㎡）、二手房的交易套数，即截至本周六共56天。
            current_week = recent8_week_nums[-1]
            last_week = recent8_week_nums[-2]
            recent4weeks: List[str] = recent8_week_nums[4:]
            recent4weeks_v2: List[str] = [week.replace('第', '') for week in recent4weeks]
            recent4weeks_dict: Dict[str, str] = dict(zip(recent4weeks, recent4weeks_v2))
            mom_column = f'{recent4weeks_v2[-1]}环比'
            yoy_column = f'{recent4weeks_v2[-1]}同比'
            newhouse_last_year_same_week_column = f'去年{current_week}成交面积'
            secondhouse_last_year_same_week_column = f'去年{current_week}成交套数'

            # 新房：按梯队、城市、周度数分组统计近4周的交易面积（万㎡）
            recent4week_newhouse_deal_df = recent8week_newhouse_deal_df[
                recent8week_newhouse_deal_df['周度数'].isin(recent4weeks)]
            newhouse_deal_bylevel_bycity_df = (recent4week_newhouse_deal_df
                                               .groupby(['梯队', '城市', '周度数'])['成交面积'].sum())
            newhouse_deal_bylevel_bycity_df.columns = ['梯队', '城市', '周度数', '成交面积']
            newhouse_deal_bylevel_bycity_pivot_df = newhouse_deal_bylevel_bycity_df.reset_index().pivot(
                index=['梯队', '城市'], columns='周度数', values='成交面积')
            last_year_same_week_newhouse_deal_bylevel_bycity_df = (last_year_same_week_newhouse_deal_df
                                                                   .groupby(['梯队', '城市'])['成交面积'].sum())
            last_year_same_week_newhouse_deal_bylevel_bycity_df.columns = ['梯队', '城市', '成交面积']
            newhouse_deal_bylevel_bycity_pivot_df = newhouse_deal_bylevel_bycity_pivot_df.reset_index()
            newhouse_deal_bylevel_bycity_merged_df = (newhouse_deal_bylevel_bycity_pivot_df
                                                     .merge(last_year_same_week_newhouse_deal_bylevel_bycity_df,
                                                             on=['梯队', '城市'], how='left'))
            newhouse_deal_bylevel_bycity_merged_df.rename(columns={'成交面积': newhouse_last_year_same_week_column}, inplace=True)
            newhouse_deal_bylevel_bycity_merged_df[mom_column] \
                = ((newhouse_deal_bylevel_bycity_merged_df[current_week] -
                   newhouse_deal_bylevel_bycity_merged_df[last_week])
                   / newhouse_deal_bylevel_bycity_merged_df[last_week])
            newhouse_deal_bylevel_bycity_merged_df[yoy_column] \
                = ((newhouse_deal_bylevel_bycity_merged_df[current_week] -
                   newhouse_deal_bylevel_bycity_merged_df[newhouse_last_year_same_week_column])
                   / newhouse_deal_bylevel_bycity_merged_df[newhouse_last_year_same_week_column])

            # 新房：按梯队、周度数分组统计近4周的交易面积（万㎡）
            recent4week_newhouse_deal_bylevel_df: pd.DataFrame = (recent4week_newhouse_deal_df
                                                                  .groupby(['梯队', '周度数'])['成交面积'].sum())
            recent4week_newhouse_deal_bylevel_df.columns = ['梯队', '周度数', '成交面积']
            recent4week_newhouse_deal_bylevel_pivot_df = (recent4week_newhouse_deal_bylevel_df
                                                         .reset_index()
                                                         .pivot(index='梯队',
                                                                columns='周度数',
                                                                values='成交面积'))
            last_year_same_week_newhouse_deal_bylevel_df = (last_year_same_week_newhouse_deal_df
                                                    .groupby('梯队')['成交面积'].sum())
            last_year_same_week_newhouse_deal_bylevel_df.columns = ['梯队', '成交面积']
            recent4week_newhouse_deal_bylevel_pivot_df = recent4week_newhouse_deal_bylevel_pivot_df.reset_index()
            recent4week_newhouse_deal_bylevel_merged_df = (recent4week_newhouse_deal_bylevel_pivot_df
                                                           .merge(last_year_same_week_newhouse_deal_bylevel_df,
                                                                  on='梯队', how='left'))
            recent4week_newhouse_deal_bylevel_merged_df.rename(columns={'成交面积': newhouse_last_year_same_week_column}, inplace=True)
            recent4week_newhouse_deal_bylevel_merged_df[mom_column] = ((recent4week_newhouse_deal_bylevel_merged_df[current_week]
                                                                        - recent4week_newhouse_deal_bylevel_merged_df[last_week])
                                                                       / recent4week_newhouse_deal_bylevel_merged_df[last_week])
            recent4week_newhouse_deal_bylevel_merged_df[yoy_column] = ((recent4week_newhouse_deal_bylevel_merged_df[f'{current_week}']
                                                                        - recent4week_newhouse_deal_bylevel_merged_df[newhouse_last_year_same_week_column])
                                                                       / recent4week_newhouse_deal_bylevel_merged_df[newhouse_last_year_same_week_column])

            # 新房：按周度数分组统计近4周的交易面积（万㎡）
            recent4week_newhouse_deal_byweek_df = (recent4week_newhouse_deal_df
                                             .groupby('周度数')['成交面积'].sum()).reset_index()
            recent4week_newhouse_deal_byweek_df.columns = ['周度数', '成交面积']
            recent4week_newhouse_deal_byweek_pivot_df = recent4week_newhouse_deal_byweek_df.set_index('周度数').T
            recent4week_newhouse_deal_byweek_pivot_df.columns.name = None
            recent4week_newhouse_deal_byweek_pivot_df.index = ['']
            last_year_same_week_newhouse_deal_byweek_df = (last_year_same_week_newhouse_deal_df
                                                         .groupby('周度数')['成交面积'].sum()).reset_index()
            last_year_same_week_newhouse_deal_byweek_df.columns = ['周度数', '成交面积']
            last_year_same_week_newhouse_deal_byweek_df.rename(columns={'成交面积': newhouse_last_year_same_week_column}, inplace=True)
            recent4week_newhouse_deal_byweek_pivot_df[newhouse_last_year_same_week_column] \
                = last_year_same_week_newhouse_deal_byweek_df.loc[0, newhouse_last_year_same_week_column]
            recent4week_newhouse_deal_byweek_merged_df = recent4week_newhouse_deal_byweek_pivot_df
            recent4week_newhouse_deal_byweek_merged_df[mom_column] = ((recent4week_newhouse_deal_byweek_merged_df[current_week]
                                                                       - recent4week_newhouse_deal_byweek_merged_df[last_week])
                                                                      / recent4week_newhouse_deal_byweek_merged_df[last_week])
            recent4week_newhouse_deal_byweek_merged_df[yoy_column] = ((recent4week_newhouse_deal_byweek_merged_df[f'{current_week}']
                                                                       - recent4week_newhouse_deal_byweek_merged_df[newhouse_last_year_same_week_column])
                                                                      / recent4week_newhouse_deal_byweek_merged_df[newhouse_last_year_same_week_column])

            newhouse_columns = ['梯队', '城市'] + recent4weeks + [mom_column, yoy_column, newhouse_last_year_same_week_column]
            newhouse_deal_bylevel_bycity_merged_df = newhouse_deal_bylevel_bycity_merged_df[newhouse_columns]
            recent4week_newhouse_deal_bylevel_merged_df['城市'] = '整体'
            recent4week_newhouse_deal_byweek_merged_df[['梯队', '城市']] = ['全线', '整体']
            recent4week_newhouse_deal_bylevel_merged_df \
                = recent4week_newhouse_deal_bylevel_merged_df[newhouse_columns]
            recent4week_newhouse_deal_byweek_merged_df \
                = recent4week_newhouse_deal_byweek_merged_df[newhouse_columns]
            newhouse_deal_bylevel_bycity_merged_df = pd.concat([
                newhouse_deal_bylevel_bycity_merged_df,
                recent4week_newhouse_deal_bylevel_merged_df,
                recent4week_newhouse_deal_byweek_merged_df
            ], ignore_index=True)
            # 自定义排序
            sort_order = ['一线', '二线', '三四线', '全线']
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            newhouse_deal_bylevel_bycity_merged_df['梯队'] = newhouse_deal_bylevel_bycity_merged_df['梯队'].astype(category_type)
            newhouse_deal_bylevel_bycity_merged_df.sort_values(by='梯队', inplace=True)
            newhouse_deal_bylevel_bycity_merged_df = newhouse_deal_bylevel_bycity_merged_df.rename(columns=recent4weeks_dict)

            # 二手房：按梯队、城市、周度数分组统计近4周的成交套数
            recent4week_secondhouse_deal_df: pd.DataFrame = recent8week_secondhouse_deal_df[
                recent8week_secondhouse_deal_df['周度数'].isin(recent4weeks)]
            secondhouse_deal_bylevel_bycity_df = (recent4week_secondhouse_deal_df
                                               .groupby(['梯队', '城市', '周度数'])['成交套数'].sum())
            secondhouse_deal_bylevel_bycity_df.columns = ['梯队', '城市', '周度数', '成交套数']
            secondhouse_deal_bylevel_bycity_pivot_df = secondhouse_deal_bylevel_bycity_df.reset_index().pivot(
                index=['梯队', '城市'], columns='周度数', values='成交套数')
            last_year_same_week_secondhouse_deal_bycity_df = (last_year_same_week_secondhouse_deal_df
                                                            .groupby(['梯队', '城市'])['成交套数'].sum())
            last_year_same_week_secondhouse_deal_bycity_df.columns = ['梯队', '城市', '成交套数']
            secondhouse_deal_bylevel_bycity_pivot_df = secondhouse_deal_bylevel_bycity_pivot_df.reset_index()
            secondhouse_deal_bylevel_bycity_merged_df = (secondhouse_deal_bylevel_bycity_pivot_df
                                                        .merge(last_year_same_week_secondhouse_deal_bycity_df,
                                                               on=['梯队', '城市'], how='left'))
            secondhouse_deal_bylevel_bycity_merged_df.rename(columns={'成交套数': secondhouse_last_year_same_week_column}, inplace=True)
            secondhouse_deal_bylevel_bycity_merged_df[mom_column] \
                = ((secondhouse_deal_bylevel_bycity_merged_df[current_week] -
                    secondhouse_deal_bylevel_bycity_merged_df[last_week])
                    / secondhouse_deal_bylevel_bycity_merged_df[last_week])
            secondhouse_deal_bylevel_bycity_merged_df[yoy_column] \
                = ((secondhouse_deal_bylevel_bycity_merged_df[current_week] -
                    secondhouse_deal_bylevel_bycity_merged_df[secondhouse_last_year_same_week_column])
                    / secondhouse_deal_bylevel_bycity_merged_df[secondhouse_last_year_same_week_column])

            # 二手房：按梯队、周度数分组统计近4周的成交套数
            recent4week_secondhouse_deal_bylevel_df: pd.DataFrame = (recent4week_secondhouse_deal_df
                                                           .groupby(['梯队', '周度数'])['成交套数'].sum())
            recent4week_secondhouse_deal_bylevel_df.columns = ['梯队', '周度数', '成交套数']
            rencent4week_secondhouse_deal_bylevel_pivot_df = (recent4week_secondhouse_deal_bylevel_df
                                                             .reset_index()
                                                             .pivot(index='梯队',
                                                                    columns='周度数',
                                                                    values='成交套数'))
            last_year_same_week_secondhouse_deal_bylevel_df = (last_year_same_week_secondhouse_deal_df
                                                       .groupby('梯队')['成交套数'].sum())
            last_year_same_week_secondhouse_deal_bylevel_df.columns = ['梯队', '去年同周度成交套数']
            rencent4week_secondhouse_deal_bylevel_pivot_df = rencent4week_secondhouse_deal_bylevel_pivot_df.reset_index()
            recent4week_secondhouse_deal_bylevel_merged_df = (rencent4week_secondhouse_deal_bylevel_pivot_df
                                                              .merge(last_year_same_week_secondhouse_deal_bylevel_df,
                                                                     on='梯队', how='left'))
            recent4week_secondhouse_deal_bylevel_merged_df.rename(columns={'成交套数': secondhouse_last_year_same_week_column}, inplace=True)
            recent4week_secondhouse_deal_bylevel_merged_df[mom_column] = \
                ((recent4week_secondhouse_deal_bylevel_merged_df[f'{current_week}']
                  - recent4week_secondhouse_deal_bylevel_merged_df[f'{last_week}'])
                 / recent4week_secondhouse_deal_bylevel_merged_df[f'{last_week}'])
            recent4week_secondhouse_deal_bylevel_merged_df[yoy_column] = \
                ((recent4week_secondhouse_deal_bylevel_merged_df[f'{current_week}'] -
                  recent4week_secondhouse_deal_bylevel_merged_df[secondhouse_last_year_same_week_column])
                 / recent4week_secondhouse_deal_bylevel_merged_df[secondhouse_last_year_same_week_column])

            # 二手房：按周度数分组统计近4周的成交套数
            recent4week_secondhouse_deal_byweek_df = (recent4week_secondhouse_deal_df
                                                    .groupby('周度数')['成交套数'].sum()).reset_index()
            recent4week_secondhouse_deal_byweek_df.columns = ['周度数', '成交套数']
            recent4week_secondhouse_deal_byweek_pivot_df = recent4week_secondhouse_deal_byweek_df.set_index('周度数').T
            recent4week_secondhouse_deal_byweek_pivot_df.columns.name = None
            recent4week_secondhouse_deal_byweek_pivot_df.index = ['']
            last_year_same_week_secondhouse_deal_byweek_df = (last_year_same_week_secondhouse_deal_df
                                                            .groupby('周度数')['成交套数'].sum()).reset_index()
            last_year_same_week_secondhouse_deal_byweek_df.columns = ['周度数', '成交套数']
            last_year_same_week_secondhouse_deal_byweek_df.rename(columns={'成交套数': secondhouse_last_year_same_week_column}, inplace=True)
            recent4week_secondhouse_deal_byweek_pivot_df[secondhouse_last_year_same_week_column] \
                = last_year_same_week_secondhouse_deal_byweek_df.loc[0, secondhouse_last_year_same_week_column]
            recent4week_secondhouse_deal_byweek_merged_df = recent4week_secondhouse_deal_byweek_pivot_df
            recent4week_secondhouse_deal_byweek_merged_df[mom_column] = \
                ((recent4week_secondhouse_deal_byweek_merged_df[current_week]
                  - recent4week_secondhouse_deal_byweek_merged_df[last_week])
                 / recent4week_secondhouse_deal_byweek_merged_df[last_week])
            recent4week_secondhouse_deal_byweek_merged_df[yoy_column] = \
                ((recent4week_secondhouse_deal_byweek_merged_df[current_week] -
                  recent4week_secondhouse_deal_byweek_merged_df[secondhouse_last_year_same_week_column])
                 / recent4week_secondhouse_deal_byweek_merged_df[secondhouse_last_year_same_week_column])

            secondhouse_columns = ['梯队', '城市'] + recent4weeks + [mom_column, yoy_column, secondhouse_last_year_same_week_column]
            secondhouse_deal_bylevel_bycity_merged_df = secondhouse_deal_bylevel_bycity_merged_df[secondhouse_columns]
            recent4week_secondhouse_deal_bylevel_merged_df['城市'] = '整体'
            recent4week_secondhouse_deal_byweek_merged_df[['梯队', '城市']] = ['全线', '整体']
            recent4week_secondhouse_deal_bylevel_merged_df \
                = recent4week_secondhouse_deal_bylevel_merged_df[secondhouse_columns]
            recent4week_secondhouse_deal_byweek_merged_df \
                = recent4week_secondhouse_deal_byweek_merged_df[secondhouse_columns]
            secondhouse_deal_bylevel_bycity_merged_df = pd.concat([
                secondhouse_deal_bylevel_bycity_merged_df,
                recent4week_secondhouse_deal_bylevel_merged_df,
                recent4week_secondhouse_deal_byweek_merged_df
            ], ignore_index=True)
            # 自定义排序
            sort_order = ['一线', '二线', '三四线', '全线']
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            secondhouse_deal_bylevel_bycity_merged_df['梯队'] = secondhouse_deal_bylevel_bycity_merged_df['梯队'].astype(category_type)
            secondhouse_deal_bylevel_bycity_merged_df.sort_values(by='梯队', inplace=True)
            secondhouse_deal_bylevel_bycity_merged_df = secondhouse_deal_bylevel_bycity_merged_df.rename(columns=recent4weeks_dict)

            # 新房月度（从月初1号至本周六）梯队同环比计算
            current_month_newhouse_sum_df = (current_month_newhouse_deal_df
                                .groupby('梯队')['成交面积'].sum()).reset_index()
            current_month_newhouse_sum_df.columns = ['梯队', '本月成交面积']
            last_month_newhouse_deal_df = (last_month_newhouse_deal_df
                                            .groupby('梯队')['成交面积'].sum()).reset_index()
            last_month_newhouse_deal_df.columns = ['梯队', '上月成交面积']
            last_year_same_month_newhouse_deal_df = (last_year_same_month_newhouse_deal_df
                                                    .groupby('梯队')['成交面积'].sum()).reset_index()
            last_year_same_month_newhouse_deal_df.columns = ['梯队', '去年同月成交面积']
            merged_month_newhouse_df = current_month_newhouse_sum_df.merge(last_month_newhouse_deal_df, on='梯队', how='left')
            merged_month_newhouse_df = merged_month_newhouse_df.merge(last_year_same_month_newhouse_deal_df, on='梯队', how='left')
            merged_month_newhouse_df['环比'] = ((merged_month_newhouse_df['本月成交面积'] - merged_month_newhouse_df['上月成交面积'])
                                               / merged_month_newhouse_df['上月成交面积'])
            merged_month_newhouse_df['同比'] = ((merged_month_newhouse_df['本月成交面积'] - merged_month_newhouse_df['去年同月成交面积'])
                                               / merged_month_newhouse_df['去年同月成交面积'])
            # 计算全线同环比
            current_month_newhouse_sum_deal = current_month_newhouse_sum_df['本月成交面积'].sum()
            last_month_newhouse_sum_deal = last_month_newhouse_deal_df['上月成交面积'].sum()
            last_year_same_month_newhouse_sum_deal = last_year_same_month_newhouse_deal_df['去年同月成交面积'].sum()
            month_on_month_ratio = ((current_month_newhouse_sum_deal - last_month_newhouse_sum_deal)
                                    / last_month_newhouse_sum_deal)
            year_on_year_ratio = ((current_month_newhouse_sum_deal - last_year_same_month_newhouse_sum_deal)
                                  / last_year_same_month_newhouse_sum_deal)
            merged_month_newhouse_df.loc[len(merged_month_newhouse_df), :] \
                = ['全线', current_month_newhouse_sum_deal,
                   last_month_newhouse_sum_deal,
                   last_year_same_month_newhouse_sum_deal,
                   month_on_month_ratio, year_on_year_ratio]
            # 自定义排序
            sort_order = ['一线', '二线', '三四线', '全线']
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            merged_month_newhouse_df['梯队'] = merged_month_newhouse_df['梯队'].astype(category_type)
            merged_month_newhouse_df.sort_values(by='梯队', inplace=True)

            # 二手房月度（从月初1号至本周六）梯队同环比计算
            current_month_secondhouse_sum_df = (current_month_secondhouse_deal_df
                                               .groupby('梯队')['成交套数'].sum()).reset_index()
            current_month_secondhouse_sum_df.columns = ['梯队', '本月成交套数']
            last_month_secondhouse_deal_df = (last_month_secondhouse_deal_df
                                             .groupby('梯队')['成交套数'].sum()).reset_index()
            last_month_secondhouse_deal_df.columns = ['梯队', '上月成交套数']
            last_year_same_month_secondhouse_deal_df = (last_year_same_month_secondhouse_deal_df
                                                       .groupby('梯队')['成交套数'].sum()).reset_index()
            last_year_same_month_secondhouse_deal_df.columns = ['梯队', '去年同月成交套数']
            merged_month_secondhouse_df = current_month_secondhouse_sum_df.merge(last_month_secondhouse_deal_df, on='梯队', how='left')
            merged_month_secondhouse_df = merged_month_secondhouse_df.merge(last_year_same_month_secondhouse_deal_df, on='梯队', how='left')
            merged_month_secondhouse_df['环比'] = ((merged_month_secondhouse_df['本月成交套数'] - merged_month_secondhouse_df['上月成交套数'])
                                                  / merged_month_secondhouse_df['上月成交套数'])
            merged_month_secondhouse_df['同比'] = ((merged_month_secondhouse_df['本月成交套数'] - merged_month_secondhouse_df['去年同月成交套数'])
                                                  / merged_month_secondhouse_df['去年同月成交套数'])
            # 计算全线同环比
            current_month_secondhouse_sum_deal = current_month_secondhouse_sum_df['本月成交套数'].sum()
            last_month_secondhouse_sum_deal = last_month_secondhouse_deal_df['上月成交套数'].sum()
            last_year_same_month_secondhouse_sum_deal = last_year_same_month_secondhouse_deal_df['去年同月成交套数'].sum()
            month_on_month_ratio = ((current_month_secondhouse_sum_deal - last_month_secondhouse_sum_deal)
                                    / last_month_secondhouse_sum_deal)
            year_on_year_ratio = ((current_month_secondhouse_sum_deal - last_year_same_month_secondhouse_sum_deal)
                                  / last_year_same_month_secondhouse_sum_deal)
            merged_month_secondhouse_df.loc[len(merged_month_secondhouse_df), :] \
                = ['全线', current_month_secondhouse_sum_deal,
                   last_month_secondhouse_sum_deal,
                   last_year_same_month_secondhouse_sum_deal,
                   month_on_month_ratio, year_on_year_ratio]
            # 自定义排序
            sort_order = ['一线', '二线', '三四线', '全线']
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            merged_month_secondhouse_df['梯队'] = merged_month_secondhouse_df['梯队'].astype(category_type)
            merged_month_secondhouse_df.sort_values(by='梯队', inplace=True)

            # 可售面积环比
            current_week_newhouse_available_df.loc[len(current_week_newhouse_available_df), :] \
                = ['整体',
                   current_week_newhouse_available_df['可售套数'].sum(),
                   current_week_newhouse_available_df['可售面积'].sum(),
                   '全线']
            last_week_newhouse_available_df.loc[len(last_week_newhouse_available_df), :] \
                = ['整体',
                   last_week_newhouse_available_df['可售套数'].sum(),
                   last_week_newhouse_available_df['可售面积'].sum(),
                   '全线']
            current_week_newhouse_available_df.rename(columns={'可售套数': '本周可售套数',
                                                               '可售面积': '本周可售面积'}, inplace=True)
            last_week_newhouse_available_df.rename(columns={'可售套数': '上周可售套数',
                                                            '可售面积': '上周可售面积'}, inplace=True)
            available_df: pd.DataFrame = current_week_newhouse_available_df.merge(last_week_newhouse_available_df,
                                                                         on=['梯队', '城市'], how='left')
            available_df['可售面积环比'] = available_df.apply(
                lambda x: (x['本周可售面积'] - x['上周可售面积']) / x['上周可售面积'], axis=1)
            available_df.drop(columns=['上周可售套数', '上周可售面积'], inplace=True)
            available_df.rename(columns={'本周可售套数': '可售套数', '本周可售面积': '可售面积'}, inplace=True)
            available_columns = ['梯队', '城市', '可售套数', '可售面积', '可售面积环比']
            available_df = available_df[available_columns]
            # 自定义排序
            sort_order = ['一线', '二线', '全线']
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            available_df['梯队'] = available_df['梯队'].astype(category_type)
            available_df.sort_values(by=['梯队', '可售面积环比'], inplace=True)

            # 生成新房二手房近8周成交趋势柱状图
            self.common_utils.gen_deal_trade_charts(
                newhouse_deal_df=recent8week_newhouse_deal_byweek_df,
                secondhouse_deal_df=recent8week_secondhouse_deal_byweek_df,
                save_path=r'data_files',
                date_flag='w'
            )

            # 将每个统计表存储到同一个Excel文件的不同sheet中
            with pd.ExcelWriter(f'data_files/报告数据-周度.xlsx') as writer:
                recent8week_newhouse_deal_byweek_df.to_excel(writer, sheet_name='近8周新房交易面积', index=False)
                recent8week_secondhouse_deal_byweek_df.to_excel(writer, sheet_name='近8周二手房交易套数', index=False)
                newhouse_deal_bylevel_bycity_merged_df.to_excel(writer, sheet_name='近4周新房交易面积(同环比)', index=False)
                secondhouse_deal_bylevel_bycity_merged_df.to_excel(writer, sheet_name='近4周二手房交易套数(同环比)', index=False)
                merged_month_newhouse_df.to_excel(writer, sheet_name='新房月度交易面积(按梯队)', index=False)
                merged_month_secondhouse_df.to_excel(writer, sheet_name='二手房月度交易套数(按梯队)', index=False)
                available_df.to_excel(writer, sheet_name='周度可售', index=False)

        elif self.time_flag == 'm':  # 月度数据统计
            current_month_first_day: str = pd.to_datetime(self.report_date).replace(day=1).strftime('%Y-%m-%d')
            current_month_last_day: str = self.report_date
            last_month_last_day: str = (pd.to_datetime(current_month_first_day).replace(day=1)
                                        - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            last_month_first_day: str = pd.to_datetime(last_month_last_day).replace(day=1).strftime('%Y-%m-%d')
            current_year_first_day: str = pd.to_datetime(self.report_date).replace(month=1, day=1).strftime('%Y-%m-%d')
            current_month_value: int = pd.to_datetime(self.report_date).month
            current_year_value: int = pd.to_datetime(self.report_date).year
            # 根据当前月份分别计算前6个月、前4个月的第一天
            recent6_month_first_day: str = (pd.to_datetime(current_month_first_day)
                                            - pd.DateOffset(months=5)).strftime('%Y-%m-%d')
            recent4_month_first_day: str = (pd.to_datetime(current_month_first_day)
                                            - pd.DateOffset(months=4)).strftime('%Y-%m-%d')
            last_year_same_month_first_day: str = (pd.to_datetime(current_month_first_day)
                                                   .replace(year=current_year_value-1).strftime('%Y-%m-%d'))
            # 如果当前月份为2月，则根据当年年度值是闰年还是平年，确定去年同月份的天数
            last_year_same_month_last_day: str | None = None
            if current_month_value == 2:
                if calendar.isleap(current_year_value):
                    last_year_same_month_last_day = f'{current_year_value-1}-02-29'
                else:
                    last_year_same_month_last_day = f'{current_year_value-1}-02-28'
            else:
                last_year_same_month_last_day = (pd.to_datetime(current_month_last_day)
                                                 .replace(year=current_year_value-1).strftime('%Y-%m-%d'))
            last_year_first_day: str = (pd.to_datetime(current_year_first_day)
                                        .replace(year=current_year_value-1).strftime('%Y-%m-%d'))

            # 新房本年度交易数据统计
            # 如果当前月份小于6，则这里统计近6个月的数据
            if pd.to_datetime(current_month_first_day).month < 6:
                current_year_newhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                    city_list=newhouse_deal_city_list,
                    start_date=recent6_month_first_day,
                    end_date=current_month_last_day
                )
                db_current_year_newhouse_deal_df: pd.DataFrame = self.get_newhouse_daily_deal_data(
                    start_date=recent6_month_first_day,
                    end_date=current_month_last_day
                )
                current_year_newhouse_deal_df = current_year_newhouse_deal_df.merge(
                    db_current_year_newhouse_deal_df, on=['城市', '数据日期'], how='left'
                )
            else:
                current_year_newhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                    city_list=newhouse_deal_city_list,
                    start_date=current_year_first_day,
                    end_date=current_month_last_day
                )
                db_current_year_newhouse_deal_df: pd.DataFrame = self.get_newhouse_daily_deal_data(
                    start_date=current_year_first_day,
                    end_date=current_month_last_day
                )
                current_year_newhouse_deal_df = current_year_newhouse_deal_df.merge(
                    db_current_year_newhouse_deal_df, on=['城市', '数据日期'], how='left'
                )

            # 新房去年度交易数据统计
            last_year_newhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                city_list=newhouse_deal_city_list,
                start_date=last_year_first_day,
                end_date=last_year_same_month_last_day
            )
            db_last_year_newhouse_deal_df: pd.DataFrame = self.get_newhouse_daily_deal_data(
                start_date=last_year_first_day,
                end_date=last_year_same_month_last_day
            )
            last_year_newhouse_deal_df = last_year_newhouse_deal_df.merge(
                db_last_year_newhouse_deal_df, on=['城市', '数据日期'], how='left'
            )

            # 新房本月可售数据统计
            current_month_newhouse_available_df: pd.DataFrame = self.get_newhouse_available_data(current_month_last_day)

            # 新房上月可售数据统计
            last_month_newhouse_available_df: pd.DataFrame = self.get_newhouse_available_data(last_month_last_day)

            # 二手房本年度交易数据统计
            # 如果当前月份小于6，则这里统计近6个月的数据
            if pd.to_datetime(current_month_first_day).month < 6:
                current_year_secondhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                    city_list=secondhouse_deal_city_list,
                    start_date=recent6_month_first_day,
                    end_date=current_month_last_day
                )
                db_current_year_secondhouse_deal_df: pd.DataFrame = self.get_secondhouse_daily_deal_data(
                    start_date=recent6_month_first_day,
                    end_date=current_month_last_day
                )
                current_year_secondhouse_deal_df = current_year_secondhouse_deal_df.merge(
                    db_current_year_secondhouse_deal_df, on=['城市', '数据日期'], how='left'
                )
            else:
                current_year_secondhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                    city_list=secondhouse_deal_city_list,
                    start_date=current_year_first_day,
                    end_date=current_month_last_day
                )
                db_current_year_secondhouse_deal_df: pd.DataFrame = self.get_secondhouse_daily_deal_data(
                    start_date=current_year_first_day,
                    end_date=current_month_last_day
                )
                current_year_secondhouse_deal_df = current_year_secondhouse_deal_df.merge(
                    db_current_year_secondhouse_deal_df, on=['城市', '数据日期'], how='left'
                )

            # 二手房去年度交易数据统计
            last_year_secondhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
                city_list=secondhouse_deal_city_list,
                start_date=last_year_first_day,
                end_date=last_year_same_month_last_day
            )
            db_last_year_secondhouse_deal_df: pd.DataFrame = self.get_secondhouse_daily_deal_data(
                start_date=last_year_first_day,
                end_date=last_year_same_month_last_day
            )
            last_year_secondhouse_deal_df = last_year_secondhouse_deal_df.merge(
                db_last_year_secondhouse_deal_df, on=['城市', '数据日期'], how='left'
            )

            # 如果当月是1月，则上月数据需单独统计，用于计算环比
            # if pd.to_datetime(current_month_first_day).month == 1:
            #     # 上月新房交易数据
            #     last_month_newhouse_deal_df = self.common_utils.generate_continous_data(
            #         city_list=newhouse_deal_city_list,
            #         start_date=last_month_first_day,
            #         end_date=last_month_last_day
            #     )
            #     db_last_month_newhouse_deal_df: pd.DataFrame = self.get_newhouse_daily_deal_data(
            #         start_date=last_month_first_day,
            #         end_date=last_month_last_day
            #     )
            #     last_month_newhouse_deal_df = last_month_newhouse_deal_df.merge(
            #         db_last_month_newhouse_deal_df, on=['城市', '数据日期'], how='left'
            #     )
            #     if not last_month_newhouse_deal_df.empty:
            #         current_year_newhouse_deal_df = pd.concat([current_year_newhouse_deal_df,
            #                                                    last_month_newhouse_deal_df], ignore_index=True)
            #     # 上月二手房交易数据
            #     last_month_secondhouse_deal_df: pd.DataFrame = self.common_utils.generate_continous_data(
            #         city_list=last_year_first_day,
            #         start_date=last_month_first_day,
            #         end_date=last_month_last_day
            #     )
            #     db_last_month_secondhouse_deal_df: pd.DataFrame = self.get_secondhouse_daily_deal_data(
            #         start_date=last_month_first_day,
            #         end_date=last_month_last_day
            #     )
            #     last_month_secondhouse_deal_df = last_month_secondhouse_deal_df.merge(
            #         db_last_month_secondhouse_deal_df, on=['城市', '数据日期'], how='left'
            #     )
            #     if not last_month_secondhouse_deal_df.empty:
            #         current_year_secondhouse_deal_df = pd.concat([current_year_secondhouse_deal_df,
            #                                                       last_month_secondhouse_deal_df], ignore_index=True)

            # 衢州新房成交缺数处理：节假日或周末缺数默认补0
            current_year: int = pd.to_datetime(current_month_last_day).year
            holiday_dates: List[datetime.date] = self.common_utils.get_cn_holidays([current_year - 1, current_year])
            current_year_newhouse_deal_df.loc[((current_year_newhouse_deal_df['城市'] == '衢州')
                                               & (current_year_newhouse_deal_df['成交面积'].isnull())), '成交面积'] \
                = (current_year_newhouse_deal_df.loc[((current_year_newhouse_deal_df['城市'] == '衢州')
                                                      & (current_year_newhouse_deal_df['成交面积'].isnull())), :]
                   .apply(lambda x: 0 if x['数据日期'].date() in holiday_dates
                                         or x['数据日期'].weekday() in (5, 6) else x['成交面积'], axis=1))
            last_year_newhouse_deal_df.loc[((last_year_newhouse_deal_df['城市'] == '衢州')
                                            & (last_year_newhouse_deal_df['成交面积'].isnull())), '成交面积'] \
                = (last_year_newhouse_deal_df.loc[((last_year_newhouse_deal_df['城市'] == '衢州')
                                                   & (last_year_newhouse_deal_df['成交面积'].isnull())), :]
                   .apply(lambda x: 0 if x['数据日期'].date() in holiday_dates
                                         or x['数据日期'].weekday() in (5, 6) else x['成交面积'], axis=1))

            # 临时补充缺失日度数据
            cric_official_daily_data_df: pd.DataFrame = pd.read_excel('data_files/克而瑞官方发布日度数据.xlsx')
            cric_official_daily_data_df['数据日期'] = pd.to_datetime(cric_official_daily_data_df['数据日期'])
            newhouse_daily_deal_df = cric_official_daily_data_df[['城市', '数据日期', '成交面积/套数']][cric_official_daily_data_df['数据类型'] == '新房']
            newhouse_daily_deal_df = newhouse_daily_deal_df.rename(columns={'成交面积/套数': '成交面积'})
            secondhouse_daily_deal_df = cric_official_daily_data_df[cric_official_daily_data_df['数据类型'] == '二手房']
            secondhouse_daily_deal_df = secondhouse_daily_deal_df.rename(columns={'成交面积/套数': '成交套数'})
            newhouse_daily_deal_dict: dict = newhouse_daily_deal_df.set_index(['城市', '数据日期'])['成交面积'].to_dict()
            secondhouse_daily_deal_dict: dict = secondhouse_daily_deal_df.set_index(['城市', '数据日期'])['成交套数'].to_dict()
            current_year_newhouse_deal_df['成交面积'] = current_year_newhouse_deal_df.apply(
                lambda x: newhouse_daily_deal_dict.get((x['城市'], x['数据日期']), None)
                if pd.isnull(x['成交面积']) else x['成交面积'], axis=1)
            last_year_newhouse_deal_df['成交面积'] = last_year_newhouse_deal_df.apply(
                lambda x: newhouse_daily_deal_dict.get((x['城市'], x['数据日期']), None)
                if pd.isnull(x['成交面积']) else x['成交面积'], axis=1)
            current_year_secondhouse_deal_df['成交套数'] = current_year_secondhouse_deal_df.apply(
                lambda x: secondhouse_daily_deal_dict.get((x['城市'], x['数据日期']), None)
                if pd.isnull(x['成交套数']) else x['成交套数'], axis=1)
            last_year_secondhouse_deal_df['成交套数'] = last_year_secondhouse_deal_df.apply(
                lambda x: secondhouse_daily_deal_dict.get((x['城市'], x['数据日期']), None)
                if pd.isnull(x['成交套数']) else x['成交套数'], axis=1)
            
            # 存储上述统计结果，用于补数确认
            with pd.ExcelWriter(f'data_files/报告原始日度数据-月度.xlsx') as writer:
                current_year_newhouse_deal_df.to_excel(writer, sheet_name='新房-本年度交易', index=False)
                last_year_newhouse_deal_df.to_excel(writer, sheet_name='新房-去年交易', index=False)
                current_month_newhouse_available_df.to_excel(writer, sheet_name='新房-本月可售', index=False)
                last_month_newhouse_available_df.to_excel(writer, sheet_name='新房-上月可售', index=False)
                current_year_secondhouse_deal_df.to_excel(writer, sheet_name='二手房-本年度交易', index=False)
                last_year_secondhouse_deal_df.to_excel(writer, sheet_name='二手房-去年交易', index=False)

            # 缺数天数统计
            current_year_newhouse_deal_df['缺数'] = current_year_newhouse_deal_df['成交面积'].isnull().astype(int)
            last_year_newhouse_deal_df['缺数'] = last_year_newhouse_deal_df['成交面积'].isnull().astype(int)
            current_year_secondhouse_deal_df['缺数'] = current_year_secondhouse_deal_df['成交套数'].isnull().astype(int)
            last_year_secondhouse_deal_df['缺数'] = last_year_secondhouse_deal_df['成交套数'].isnull().astype(int)
            current_month_newhouse_available_shortage_city_list: List[str] = list(
                set(newhouse_available_city_list).difference(set(current_month_newhouse_available_df['城市'].tolist()))
            )
            last_month_newhouse_available_shortage_city_list: List[str] = list(
                set(newhouse_available_city_list).difference(set(last_month_newhouse_available_df['城市'].tolist()))
            )
            print('本年度新房缺数情况：')
            print(current_year_newhouse_deal_df.groupby('城市')['缺数'].sum())
            print('去年新房缺数情况：')
            print(last_year_newhouse_deal_df.groupby('城市')['缺数'].sum())
            print('本年度二手房缺数情况：')
            print(current_year_secondhouse_deal_df.groupby('城市')['缺数'].sum())
            print('去年二手房缺数情况：')
            print(last_year_secondhouse_deal_df.groupby('城市')['缺数'].sum())
            print(f'本月新房可售缺数城市：{current_month_newhouse_available_shortage_city_list}')
            print(f'上月新房可售缺数城市：{last_month_newhouse_available_shortage_city_list}')

            prompt_msg: str = f"""
                        原始数据已存储至【data_files/报告原始日度数据-月度.xlsx】。
                        请确认以上数据是否正确，若数据有缺失，请确认补数方法：
                        1--人工手动补数；2--程序自动补数（待开发）。
                        注意，人工手动补数完成后请保存并关闭文件；若无需补数，请回车继续。
                        """
            prompt_input: str = input(textwrap.dedent(prompt_msg))
            if prompt_input == '1':
                # 手动补数，重新加载补数后的文件
                data_dfs: Dict[str, pd.DataFrame] = pd.read_excel(f'data_files/报告原始日度数据-月度.xlsx',
                                                                  sheet_name=None)
                current_year_newhouse_deal_df = data_dfs['新房-本年度交易']
                last_year_newhouse_deal_df = data_dfs['新房-去年交易']
                current_month_newhouse_available_df = data_dfs['新房-本月可售']
                last_month_newhouse_available_df = data_dfs['新房-上月可售']
                current_year_secondhouse_deal_df = data_dfs['二手房-本年度交易']
                last_year_secondhouse_deal_df = data_dfs['二手房-去年交易']
            elif prompt_input == '2':
                # 自动补数，待开发
                sys.exit()
            else:
                pass

            # 为上述统计数据增加"梯队"和"月份"
            city_level_df: pd.DataFrame = self.get_config()['城市梯队']
            city_level_dict: Dict[str, str] = city_level_df.set_index('城市')['梯队'].to_dict()
            current_year_newhouse_deal_df['梯队'] = current_year_newhouse_deal_df['城市'].map(city_level_dict)
            current_year_newhouse_deal_df['月份'] = pd.to_datetime(current_year_newhouse_deal_df['数据日期']).apply(lambda x: x.strftime('%Y-%#m'))
            last_year_newhouse_deal_df['梯队'] = last_year_newhouse_deal_df['城市'].map(city_level_dict)
            last_year_newhouse_deal_df['月份'] = pd.to_datetime(last_year_newhouse_deal_df['数据日期']).apply(lambda x: x.strftime('%Y-%#m'))
            current_month_newhouse_available_df['梯队'] = current_month_newhouse_available_df['城市'].map(city_level_dict)
            last_month_newhouse_available_df['梯队'] = last_month_newhouse_available_df['城市'].map(city_level_dict)
            current_year_secondhouse_deal_df['梯队'] = current_year_secondhouse_deal_df['城市'].map(city_level_dict)
            current_year_secondhouse_deal_df['月份'] = pd.to_datetime(current_year_secondhouse_deal_df['数据日期']).apply(lambda x: x.strftime('%Y-%#m'))
            last_year_secondhouse_deal_df['梯队'] = last_year_secondhouse_deal_df['城市'].map(city_level_dict)
            last_year_secondhouse_deal_df['月份'] = pd.to_datetime(last_year_secondhouse_deal_df['数据日期']).apply(lambda x: x.strftime('%Y-%#m'))

            # 根据报告时间计算近6个月的月度时间（年-月），并存储在列表中
            recent6_month_list: List[str] = []
            for i in range(6):
                recent6_month_list.append((pd.to_datetime(current_month_first_day)
                                           - pd.DateOffset(months=i)).strftime('%Y-%#m'))
            # 月份逆序排列（从小到大排序）
            recent6_month_list.reverse()
            recent6_month_v2_list: List[str] = [month.split('-')[1] + '月' for month in recent6_month_list]
            recent6_month_dict: Dict[str, str] = dict(zip(recent6_month_list, recent6_month_v2_list))
            current_month: str = recent6_month_list[-1]
            last_month: str = recent6_month_list[-2]
            newhouse_last_year_same_month_column: str = f'去年{current_month.split("-")[1]}月成交面积'
            secondhouse_last_year_same_month_column: str = f'去年{current_month.split("-")[1]}月成交套数'
            mom_column: str = f'{recent6_month_v2_list[-1]}环比'
            yoy_column: str = f'{recent6_month_v2_list[-1]}同比'

            # 相关基表准备
            # 新房交易：统计近6个月的交易面积（万㎡）
            recent6_month_newhouse_deal_df: pd.DataFrame = current_year_newhouse_deal_df[
                current_year_newhouse_deal_df['月份'].isin(recent6_month_list)]
            # 新房交易：统计近4个月的交易面积（万㎡）
            recent4_month_newhouse_deal_df: pd.DataFrame = current_year_newhouse_deal_df[
                current_year_newhouse_deal_df['月份'].isin(recent6_month_list[-4:])
            ]
            # 新房交易：统计当月的交易面积（万㎡）
            current_month_newhouse_deal_df: pd.DataFrame = current_year_newhouse_deal_df[
                (current_year_newhouse_deal_df['数据日期'] >= current_month_first_day)
                & (current_year_newhouse_deal_df['数据日期'] <= current_month_last_day)
            ]
            # 新房交易：统计上月的交易面积（万㎡）
            last_month_newhouse_deal_df: pd.DataFrame = current_year_newhouse_deal_df[
                (current_year_newhouse_deal_df['数据日期'] >= last_month_first_day)
                & (current_year_newhouse_deal_df['数据日期'] <= last_month_last_day)
            ]
            # 新房交易：统计去年同月的交易面积（万㎡）
            last_year_same_month_newhouse_deal_df: pd.DataFrame = last_year_newhouse_deal_df[
                (last_year_newhouse_deal_df['数据日期'] >= last_year_same_month_first_day)
                & (last_year_newhouse_deal_df['数据日期'] <= last_year_same_month_last_day)
            ]
            # 二手房交易：统计近6个月的交易套数
            recent6_month_secondhouse_deal_df: pd.DataFrame = current_year_secondhouse_deal_df[
                current_year_secondhouse_deal_df['月份'].isin(recent6_month_list)]
            # 二手房交易：统计近4个月的交易套数
            recent4_month_secondhouse_deal_df: pd.DataFrame = current_year_secondhouse_deal_df[
                current_year_secondhouse_deal_df['月份'].isin(recent6_month_list[-4:])
            ]
            # 二手房交易：统计当月的交易套数
            current_month_secondhouse_deal_df: pd.DataFrame = current_year_secondhouse_deal_df[
                (current_year_secondhouse_deal_df['数据日期'] >= current_month_first_day)
                & (current_year_secondhouse_deal_df['数据日期'] <= current_month_last_day)
            ]
            # 二手房交易：统计上月的交易套数
            last_month_secondhouse_deal_df: pd.DataFrame = last_year_secondhouse_deal_df[
                (last_year_secondhouse_deal_df['数据日期'] >= last_month_first_day)
                & (last_year_secondhouse_deal_df['数据日期'] <= last_month_last_day)
            ]
            # 二手房交易：统计去年同月的交易套数
            last_year_same_month_secondhouse_deal_df: pd.DataFrame = last_year_secondhouse_deal_df[
                (last_year_secondhouse_deal_df['数据日期'] >= last_year_same_month_first_day)
                & (last_year_secondhouse_deal_df['数据日期'] <= last_year_same_month_last_day)
            ]

            # 计算同环比需要剔除的城市
            current_month_days: int = pd.to_datetime(current_month_last_day).day
            last_month_days: int = pd.to_datetime(last_month_last_day).day
            last_year_same_month_days: int = pd.to_datetime(last_year_same_month_last_day).day
            current_year_days: int = pd.to_datetime(current_month_last_day).dayofyear
            last_year_days: int = pd.to_datetime(last_year_same_month_last_day).dayofyear
            current_month_newhouse_zero_deal_city_list: List[str] = (
                current_month_newhouse_deal_df[current_month_newhouse_deal_df['成交面积'] == 0]
                .groupby('城市')['成交面积'].count()
                .loc[lambda x: x > current_month_days / 3].index.tolist()
            )
            last_month_zero_newhouse_deal_city_list: List[str] = (
                last_month_newhouse_deal_df[last_month_newhouse_deal_df['成交面积'] == 0]
                .groupby('城市')['成交面积'].count()
                .loc[lambda x: x > last_month_days / 3].index.tolist()
            )
            last_year_same_month_newhouse_zero_deal_city_list: List[str] = (
                last_year_same_month_newhouse_deal_df[last_year_same_month_newhouse_deal_df['成交面积'] == 0]
                .groupby('城市')['成交面积'].count()
                .loc[lambda x: x > last_year_same_month_days / 3].index.tolist()
            )
            current_year_zero_newhouse_deal_city_list: List[str] = list(
                current_year_newhouse_deal_df[(current_year_newhouse_deal_df['成交面积'] == 0)
                                              & (current_year_newhouse_deal_df['数据日期'] >= current_year_first_day)
                                              & (current_year_newhouse_deal_df['数据日期'] <= current_month_last_day)]
                .groupby('城市')['成交面积'].count()
                .loc[lambda x: x > current_year_days / 3].index.tolist()
            )
            last_year_zero_newhouse_deal_city_list: List[str] = list(
                last_year_newhouse_deal_df[last_year_newhouse_deal_df['成交面积'] == 0]
                .groupby('城市')['成交面积'].count()
                .loc[lambda x: x > last_year_days / 3].index.tolist()
            )
            newhouse_month_mom_exclude_city_list: List[str] = list(
                set(current_month_newhouse_zero_deal_city_list).union(set(last_month_zero_newhouse_deal_city_list)))
            newhouse_month_yoy_exclude_city_list: List[str] = list(
                set(current_month_newhouse_zero_deal_city_list).union(
                    set(last_year_same_month_newhouse_zero_deal_city_list)))
            newhouse_year_exclude_city_list: List[str] = list(
                set(current_year_zero_newhouse_deal_city_list).union(set(last_year_zero_newhouse_deal_city_list)))

            current_month_secondhouse_zero_deal_city_list: List[str] = (
                current_month_secondhouse_deal_df[current_month_secondhouse_deal_df['成交套数'] == 0]
                .groupby('城市')['成交套数'].count()
                .loc[lambda x: x > current_month_days / 3].index.tolist()
            )
            last_month_zero_secondhouse_deal_city_list: List[str] = (
                last_month_secondhouse_deal_df[last_month_secondhouse_deal_df['成交套数'] == 0]
                .groupby('城市')['成交套数'].count()
                .loc[lambda x: x > last_month_days / 3].index.tolist()
            )
            last_year_same_month_secondhouse_zero_deal_city_list: List[str] = (
                last_year_same_month_secondhouse_deal_df[last_year_same_month_secondhouse_deal_df['成交套数'] == 0]
                .groupby('城市')['成交套数'].count()
                .loc[lambda x: x > last_year_same_month_days / 3].index.tolist()
            )
            current_year_zero_secondhouse_deal_city_list: List[str] = list(
                current_year_secondhouse_deal_df[(current_year_secondhouse_deal_df['成交套数'] == 0)
                                                 & (current_year_secondhouse_deal_df['数据日期'] >= current_year_first_day)
                                                 & (current_year_secondhouse_deal_df['数据日期'] <= current_month_last_day)
                                                 ]
                .groupby('城市')['成交套数'].count()
                .loc[lambda x: x > current_year_days / 3].index.tolist()
            )
            last_year_zero_secondhouse_deal_city_list: List[str] = list(
                last_year_secondhouse_deal_df[last_year_secondhouse_deal_df['成交套数'] == 0]
                .groupby('城市')['成交套数'].count()
                .loc[lambda x: x > last_year_days / 3].index.tolist()
            )
            secondhouse_mom_exclude_city_list: List[str] = list(
                set(current_month_secondhouse_zero_deal_city_list).union(
                    set(last_month_zero_secondhouse_deal_city_list)))
            secondhouse_yoy_exclude_city_list: List[str] = list(
                set(current_month_secondhouse_zero_deal_city_list).union(
                    set(last_year_same_month_secondhouse_zero_deal_city_list)))
            secondhouse_year_exclude_city_list: List[str] = list(
                set(current_year_zero_secondhouse_deal_city_list).union(set(last_year_zero_secondhouse_deal_city_list)))

            # 新房：按"月份"分组统计近6个月的交易面积（万㎡）
            recent6_month_newhouse_deal_bymonth_df: pd.DataFrame = (recent6_month_newhouse_deal_df
                                                         .groupby('月份')['成交面积'].sum()).reset_index()
            recent6_month_newhouse_deal_bymonth_df.columns = ['月份', '成交面积']
            sort_order = recent6_month_list
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            recent6_month_newhouse_deal_bymonth_df['月份'] = (
                recent6_month_newhouse_deal_bymonth_df['月份'].astype(category_type))
            recent6_month_newhouse_deal_bymonth_df.sort_values(by='月份', inplace=True)
            recent6_month_newhouse_deal_bymonth_df['月份'] = recent6_month_newhouse_deal_bymonth_df['月份'].apply(lambda x: x[2:])

            # 新房：按"梯队"、"城市"、"月份"分组统计近4个月的交易面积（万㎡）
            recent4_month_newhouse_deal_belevel_bycity_bymonth_df: pd.DataFrame = (
                recent4_month_newhouse_deal_df.groupby(['梯队', '城市', '月份'])['成交面积'].sum()).reset_index()
            recent4_month_newhouse_deal_belevel_bycity_bymonth_df.columns = ['梯队', '城市', '月份', '成交面积']
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_pivot_df: pd.DataFrame = (
                recent4_month_newhouse_deal_belevel_bycity_bymonth_df
                .pivot(index=['梯队', '城市'],
                       columns='月份',
                       values='成交面积'))
            # recent4_month_newhouse_deal_bylevel_bycity_bymonth_pivot_df.columns = recent6_month_v2_list[-4:]
            last_year_same_month_newhouse_deal_bylevel_bycity_bymonth_df: pd.DataFrame = (
                last_year_same_month_newhouse_deal_df.groupby(['梯队', '城市'])['成交面积'].sum()).reset_index()
            last_year_same_month_newhouse_deal_bylevel_bycity_bymonth_df.columns = ['梯队', '城市', '成交面积']
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_pivot_df.reset_index(inplace=True)
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df: pd.DataFrame = (
                recent4_month_newhouse_deal_bylevel_bycity_bymonth_pivot_df
                .merge(last_year_same_month_newhouse_deal_bylevel_bycity_bymonth_df,
                       on=['梯队', '城市'], how='left'))
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df.rename(
                columns={'成交面积': newhouse_last_year_same_month_column}, inplace=True)
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[mom_column] = (
                (recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[current_month]
                 - recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[last_month])
                / recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[last_month])
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[yoy_column] = (
                (recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[current_month]
                 - recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[newhouse_last_year_same_month_column])
                / recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[newhouse_last_year_same_month_column])

            # 新房：按"梯队"、"月份"分组统计近4个月的交易面积（万㎡）
            recent4_month_newhouse_deal_bylevel_bymonth_df: pd.DataFrame = (
                recent4_month_newhouse_deal_df.groupby(['梯队', '月份'])['成交面积'].sum()).reset_index()
            recent4_month_newhouse_deal_bylevel_bymonth_df.columns = ['梯队', '月份', '成交面积']
            recent4_month_newhouse_deal_bylevel_bymonth_pivot_df: pd.DataFrame = (
                recent4_month_newhouse_deal_bylevel_bymonth_df
                .pivot(index='梯队', columns='月份', values='成交面积'))
            # recent4_month_newhouse_deal_bylevel_bymonth_pivot_df.columns = recent6_month_v2_list[-4:]
            last_year_same_month_newhouse_deal_bylevel_bymonth_df: pd.DataFrame = (
                last_year_same_month_newhouse_deal_df.groupby('梯队')['成交面积'].sum()).reset_index()
            last_year_same_month_newhouse_deal_bylevel_bymonth_df.columns = ['梯队', '成交面积']
            recent4_month_newhouse_deal_bylevel_bymonth_pivot_df.reset_index(inplace=True)
            recent4_month_newhouse_deal_bylevel_bymonth_merged_df: pd.DataFrame = (
                recent4_month_newhouse_deal_bylevel_bymonth_pivot_df
                .merge(last_year_same_month_newhouse_deal_bylevel_bymonth_df, on='梯队', how='left'))
            recent4_month_newhouse_deal_bylevel_bymonth_merged_df.rename(
                columns={'成交面积': newhouse_last_year_same_month_column}, inplace=True)
            recent4_month_newhouse_deal_bylevel_bymonth_merged_df[mom_column] = (
                (recent4_month_newhouse_deal_bylevel_bymonth_merged_df[current_month]
                 - recent4_month_newhouse_deal_bylevel_bymonth_merged_df[last_month])
                / recent4_month_newhouse_deal_bylevel_bymonth_merged_df[last_month])
            recent4_month_newhouse_deal_bylevel_bymonth_merged_df[yoy_column] = (
                (recent4_month_newhouse_deal_bylevel_bymonth_merged_df[current_month]
                 - recent4_month_newhouse_deal_bylevel_bymonth_merged_df[newhouse_last_year_same_month_column])
                / recent4_month_newhouse_deal_bylevel_bymonth_merged_df[newhouse_last_year_same_month_column])

            # 新房：按"月份"分组统计近4个月的交易面积（万㎡）
            recent4_month_newhouse_deal_bymonth_df: pd.DataFrame = (recent4_month_newhouse_deal_df
                                                         .groupby('月份')['成交面积'].sum()).reset_index()
            recent4_month_newhouse_deal_bymonth_df.columns = ['月份', '成交面积']
            recent4_month_newhouse_deal_bymonth_pivot_df = recent4_month_newhouse_deal_bymonth_df.set_index('月份').T
            recent4_month_newhouse_deal_bymonth_pivot_df.columns.name = None
            recent4_month_newhouse_deal_bymonth_pivot_df.index = ['']
            last_year_same_month_newhouse_deal_bymonth_df = (last_year_same_month_newhouse_deal_df
                                                           .groupby('月份')['成交面积'].sum()).reset_index()
            last_year_same_month_newhouse_deal_bymonth_df.columns = ['月份', '成交面积']
            last_year_same_month_newhouse_deal_bymonth_df.rename(columns={'成交面积': newhouse_last_year_same_month_column},
                                                               inplace=True)
            recent4_month_newhouse_deal_bymonth_pivot_df[newhouse_last_year_same_month_column] \
                = last_year_same_month_newhouse_deal_bymonth_df.loc[0, newhouse_last_year_same_month_column]
            recent4_month_newhouse_deal_bymonth_merged_df = recent4_month_newhouse_deal_bymonth_pivot_df
            recent4_month_newhouse_deal_bymonth_merged_df[mom_column] = (
                (recent4_month_newhouse_deal_bymonth_merged_df[current_month]
                 - recent4_month_newhouse_deal_bymonth_merged_df[last_month])
                / recent4_month_newhouse_deal_bymonth_merged_df[last_month])
            recent4_month_newhouse_deal_bymonth_merged_df[yoy_column] = (
                (recent4_month_newhouse_deal_bymonth_merged_df[current_month]
                 - recent4_month_newhouse_deal_bymonth_merged_df[newhouse_last_year_same_month_column])
                / recent4_month_newhouse_deal_bymonth_merged_df[newhouse_last_year_same_month_column])

            newhosue_columns: List[str] = (['梯队', '城市'] + recent6_month_list[-4:]
                                           + [mom_column, yoy_column, newhouse_last_year_same_month_column])
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df \
                = recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df[newhosue_columns]
            recent4_month_newhouse_deal_bylevel_bymonth_merged_df['城市'] = '整体'
            recent4_month_newhouse_deal_bymonth_merged_df[['梯队', '城市']] = ['全线', '整体']
            recent4_month_newhouse_deal_bylevel_bymonth_merged_df \
                = recent4_month_newhouse_deal_bylevel_bymonth_merged_df[newhosue_columns]
            recent4_month_newhouse_deal_bymonth_merged_df = recent4_month_newhouse_deal_bymonth_merged_df[newhosue_columns]
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df = pd.concat([
                recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df,
                recent4_month_newhouse_deal_bylevel_bymonth_merged_df,
                recent4_month_newhouse_deal_bymonth_merged_df
            ], ignore_index=True)
            # 自定义排序
            sort_order = ['一线', '二线', '三四线', '全线']
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df['梯队'] = (
                recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df['梯队'].astype(category_type))
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df.sort_values(by='梯队', inplace=True)
            recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df.rename(
                columns=recent6_month_dict, inplace=True
            )

            # 二手房：按"月份"分组统计近6个月的交易套数
            recent6_month_secondhouse_deal_bymonth_df: pd.DataFrame = (recent6_month_secondhouse_deal_df
                                                            .groupby('月份')['成交套数'].sum()).reset_index()
            recent6_month_secondhouse_deal_bymonth_df.columns = ['月份', '成交套数']
            sort_order = recent6_month_list
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            recent6_month_secondhouse_deal_bymonth_df['月份'] = (
                recent6_month_secondhouse_deal_bymonth_df['月份'].astype(category_type))
            recent6_month_secondhouse_deal_bymonth_df.sort_values(by='月份', inplace=True)
            recent6_month_secondhouse_deal_bymonth_df['月份'] = recent6_month_secondhouse_deal_bymonth_df['月份'].apply(lambda x: x[2:])

            # 二手房：按"梯队"、"城市"、"月份"分组统计近4个月的交易套数
            recent4_month_secondhouse_deal_belevel_bycity_bymonth_df: pd.DataFrame = (
                recent4_month_secondhouse_deal_df.groupby(['梯队', '城市', '月份'])['成交套数'].sum()).reset_index()
            recent4_month_secondhouse_deal_belevel_bycity_bymonth_df.columns = ['梯队', '城市', '月份', '成交套数']
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_pivot_df: pd.DataFrame = (
                recent4_month_secondhouse_deal_belevel_bycity_bymonth_df
                .pivot(index=['梯队', '城市'], columns='月份', values='成交套数'))
            # recent4_month_secondhouse_deal_bylevel_bycity_bymonth_pivot_df.columns = recent6_month_v2_list[-4:]
            last_year_same_month_secondhouse_deal_bylevel_bycity_bymonth_df: pd.DataFrame = (
                last_year_same_month_secondhouse_deal_df.groupby(['梯队', '城市'])['成交套数'].sum()).reset_index()
            last_year_same_month_secondhouse_deal_bylevel_bycity_bymonth_df.columns = ['梯队', '城市', '成交套数']
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_pivot_df.reset_index(inplace=True)
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df: pd.DataFrame = (
                recent4_month_secondhouse_deal_bylevel_bycity_bymonth_pivot_df
                .merge(last_year_same_month_secondhouse_deal_bylevel_bycity_bymonth_df,
                       on=['梯队', '城市'], how='left'))
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df.rename(
                columns={'成交套数': secondhouse_last_year_same_month_column}, inplace=True)
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[mom_column] = (
                (recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[current_month]
                 - recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[last_month])
                / recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[last_month])
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[yoy_column] = (
                (recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[current_month]
                 - recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[secondhouse_last_year_same_month_column])
                / recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[secondhouse_last_year_same_month_column])

            # 二手房：按"梯队"、"月份"分组统计近4个月的交易套数
            recent4_month_secondhouse_deal_bylevel_bymonth_df: pd.DataFrame = (
                recent4_month_secondhouse_deal_df.groupby(['梯队', '月份'])['成交套数'].sum()).reset_index()
            recent4_month_secondhouse_deal_bylevel_bymonth_df.columns = ['梯队', '月份', '成交套数']
            recent4_month_secondhouse_deal_bylevel_bymonth_pivot_df: pd.DataFrame = (
                recent4_month_secondhouse_deal_bylevel_bymonth_df
                .pivot(index='梯队', columns='月份', values='成交套数'))
            # recent4_month_secondhouse_deal_bylevel_bymonth_pivot_df.columns = recent6_month_v2_list[-4:]
            last_year_same_month_secondhouse_deal_bylevel_bymonth_df: pd.DataFrame = (
                last_year_same_month_secondhouse_deal_df.groupby('梯队')['成交套数'].sum()).reset_index()
            last_year_same_month_secondhouse_deal_bylevel_bymonth_df.columns = ['梯队', '成交套数']
            recent4_month_secondhouse_deal_bylevel_bymonth_pivot_df.reset_index(inplace=True)
            recent4_month_secondhouse_deal_bylevel_bymonth_merged_df: pd.DataFrame = (
                recent4_month_secondhouse_deal_bylevel_bymonth_pivot_df
                .merge(last_year_same_month_secondhouse_deal_bylevel_bymonth_df, on='梯队', how='left'))
            recent4_month_secondhouse_deal_bylevel_bymonth_merged_df.rename(
                columns={'成交套数': secondhouse_last_year_same_month_column}, inplace=True)
            recent4_month_secondhouse_deal_bylevel_bymonth_merged_df[mom_column] = (
                (recent4_month_secondhouse_deal_bylevel_bymonth_merged_df[current_month]
                 - recent4_month_secondhouse_deal_bylevel_bymonth_merged_df[last_month])
                / recent4_month_secondhouse_deal_bylevel_bymonth_merged_df[last_month])
            recent4_month_secondhouse_deal_bylevel_bymonth_merged_df[yoy_column] = (
                (recent4_month_secondhouse_deal_bylevel_bymonth_merged_df[current_month]
                 - recent4_month_secondhouse_deal_bylevel_bymonth_merged_df[secondhouse_last_year_same_month_column])
                / recent4_month_secondhouse_deal_bylevel_bymonth_merged_df[secondhouse_last_year_same_month_column])

            # 二手房：按"月份"分组统计近4个月的交易套数
            recent4_month_secondhouse_deal_bymonth_df: pd.DataFrame = (recent4_month_secondhouse_deal_df
                                                         .groupby('月份')['成交套数'].sum()).reset_index()
            recent4_month_secondhouse_deal_bymonth_df.columns = ['月份', '成交套数']
            recent4_month_secondhouse_deal_bymonth_pivot_df = recent4_month_secondhouse_deal_bymonth_df.set_index('月份').T
            recent4_month_secondhouse_deal_bymonth_pivot_df.columns.name = None
            recent4_month_secondhouse_deal_bymonth_pivot_df.index = ['']
            last_year_same_month_secondhouse_deal_bymonth_df = (last_year_same_month_secondhouse_deal_df
                                                           .groupby('月份')['成交套数'].sum()).reset_index()
            last_year_same_month_secondhouse_deal_bymonth_df.columns = ['月份', '成交套数']
            last_year_same_month_secondhouse_deal_bymonth_df.rename(
                columns={'成交套数': secondhouse_last_year_same_month_column}, inplace=True)
            recent4_month_secondhouse_deal_bymonth_pivot_df[secondhouse_last_year_same_month_column] \
                = last_year_same_month_secondhouse_deal_bymonth_df.loc[0, secondhouse_last_year_same_month_column]
            recent4_month_secondhouse_deal_bymonth_merged_df = recent4_month_secondhouse_deal_bymonth_pivot_df
            recent4_month_secondhouse_deal_bymonth_merged_df[mom_column] = (
                (recent4_month_secondhouse_deal_bymonth_merged_df[current_month]
                 - recent4_month_secondhouse_deal_bymonth_merged_df[last_month])
                / recent4_month_secondhouse_deal_bymonth_merged_df[last_month])
            recent4_month_secondhouse_deal_bymonth_merged_df[yoy_column] = (
                (recent4_month_secondhouse_deal_bymonth_merged_df[current_month]
                 - recent4_month_secondhouse_deal_bymonth_merged_df[secondhouse_last_year_same_month_column])
                / recent4_month_secondhouse_deal_bymonth_merged_df[secondhouse_last_year_same_month_column])

            secondhouse_columns: List[str] = (['梯队', '城市'] + recent6_month_list[-4:]
                                              + [mom_column, yoy_column, secondhouse_last_year_same_month_column])
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df \
                = recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[secondhouse_columns]
            recent4_month_secondhouse_deal_bylevel_bymonth_merged_df['城市'] = '整体'
            recent4_month_secondhouse_deal_bymonth_merged_df[['梯队', '城市']] = ['全线', '整体']
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df \
                = recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df[secondhouse_columns]
            recent4_month_secondhouse_deal_bymonth_merged_df \
                = recent4_month_secondhouse_deal_bymonth_merged_df[secondhouse_columns]
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df = pd.concat([
                recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df,
                recent4_month_secondhouse_deal_bylevel_bymonth_merged_df,
                recent4_month_secondhouse_deal_bymonth_merged_df
            ], ignore_index=True)
            # 自定义排序
            sort_order = ['一线', '二线', '三四线', '全线']
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df['梯队'] = (
                recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df['梯队'].astype(category_type))
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df.sort_values(by='梯队', inplace=True)
            recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df.rename(
                columns=recent6_month_dict, inplace=True
            )

            # 可售面积环比
            current_month_newhouse_available_df.loc[len(current_month_newhouse_available_df), :] \
                = ['整体',
                   current_month_newhouse_available_df['可售套数'].sum(),
                   current_month_newhouse_available_df['可售面积'].sum(),
                   '全线']
            last_month_newhouse_available_df.loc[len(last_month_newhouse_available_df), :] \
                = ['整体',
                   last_month_newhouse_available_df['可售套数'].sum(),
                   last_month_newhouse_available_df['可售面积'].sum(),
                   '全线']
            current_month_newhouse_available_df.rename(columns={'可售套数': '本周可售套数',
                                                                '可售面积': '本周可售面积'}, inplace=True)
            last_month_newhouse_available_df.rename(columns={'可售套数': '上周可售套数',
                                                             '可售面积': '上周可售面积'}, inplace=True)
            available_df: pd.DataFrame = current_month_newhouse_available_df.merge(last_month_newhouse_available_df,
                                                                                   on=['梯队', '城市'], how='left')
            available_df['可售面积环比'] = available_df.apply(
                lambda x: (x['本周可售面积'] - x['上周可售面积']) / x['上周可售面积'], axis=1)
            available_df.drop(columns=['上周可售套数', '上周可售面积'], inplace=True)
            available_df.rename(columns={'本周可售套数': '可售套数', '本周可售面积': '可售面积'}, inplace=True)
            available_columns = ['梯队', '城市', '可售套数', '可售面积', '可售面积环比']
            available_df = available_df[available_columns]
            # 自定义排序
            sort_order = ['一线', '二线', '全线']
            category_type = pd.CategoricalDtype(categories=sort_order, ordered=True)
            available_df['梯队'] = available_df['梯队'].astype(category_type)
            available_df.sort_values(by=['梯队', '可售面积环比'], inplace=True)

            # 计算新房二手房年度同比
            current_year_newhouse_deal_sum = current_year_newhouse_deal_df['成交面积'][
                (~current_year_newhouse_deal_df['城市'].isin(newhouse_year_exclude_city_list))
                & (current_year_newhouse_deal_df['数据日期'] >= current_year_first_day)
                & (current_year_newhouse_deal_df['数据日期'] <= current_month_last_day)].sum()
            last_year_newhouse_deal_sum = last_year_newhouse_deal_df['成交面积'][
                ~last_year_newhouse_deal_df['城市'].isin(newhouse_year_exclude_city_list)].sum()
            print(f'新房年度同比剔除城市：{newhouse_year_exclude_city_list}；'
                  f'本年度新房成交面积：{current_year_newhouse_deal_sum}；'
                  f'去年度新房成交面积：{last_year_newhouse_deal_sum}')
            yoy_of_newhouse_deal_annual = ((current_year_newhouse_deal_sum - last_year_newhouse_deal_sum) 
                                           / last_year_newhouse_deal_sum)
            current_year_secondhouse_deal_sum = current_year_secondhouse_deal_df['成交套数'][
                (~current_year_secondhouse_deal_df['城市'].isin(secondhouse_year_exclude_city_list))
                & (current_year_secondhouse_deal_df['数据日期'] >= current_year_first_day)
                & (current_year_secondhouse_deal_df['数据日期'] <= current_month_last_day)].sum()
            last_year_secondhouse_deal_sum = last_year_secondhouse_deal_df['成交套数'][
                ~last_year_secondhouse_deal_df['城市'].isin(secondhouse_year_exclude_city_list)].sum()
            print(f'二手房年度同比剔除城市：{secondhouse_year_exclude_city_list}；'
                  f'本年度二手房成交套数：{current_year_secondhouse_deal_sum}；'
                  f'去年度二手房成交套数：{last_year_secondhouse_deal_sum}')
            yoy_of_secondhouse_deal_annual = ((current_year_secondhouse_deal_sum - last_year_secondhouse_deal_sum)
                                              / last_year_secondhouse_deal_sum)
            new_secondhouse_deal_yoy_annual_df: pd.DataFrame = pd.DataFrame(data={'数据类型': ['新房', '二手房'],
                                                                                  '同比': [yoy_of_newhouse_deal_annual,
                                                                                         yoy_of_secondhouse_deal_annual]})

            # 生成近6个月新房二手房成交趋势柱状图
            self.common_utils.gen_deal_trade_charts(
                newhouse_deal_df=recent6_month_newhouse_deal_bymonth_df,
                secondhouse_deal_df=recent6_month_secondhouse_deal_bymonth_df,
                save_path=r'data_files',
                date_flag='m'
            )

            # 将每个统计表存储到同一个Excel文件的不同sheet中
            with pd.ExcelWriter('data_files/报告数据-月度.xlsx') as writer:
                recent6_month_newhouse_deal_bymonth_df.to_excel(writer, sheet_name='近6月新房交易面积', index=False)
                recent6_month_secondhouse_deal_bymonth_df.to_excel(writer, sheet_name='近6月二手房交易套数', index=False)
                recent4_month_newhouse_deal_bylevel_bycity_bymonth_merged_df.to_excel(writer, sheet_name='近4月新房交易面积(同环比)', index=False)
                recent4_month_secondhouse_deal_bylevel_bycity_bymonth_merged_df.to_excel(writer, sheet_name='近4月二手房交易套数(同环比)', index=False)
                available_df.to_excel(writer, sheet_name='可售面积环比', index=False)
                new_secondhouse_deal_yoy_annual_df.to_excel(writer, sheet_name='新房二手房年度交易同比', index=False)
        else:
            pass
