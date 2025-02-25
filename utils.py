import os.path
from datetime import datetime, timedelta
from typing import Tuple, List

import holidays
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rcParams


# 配置中文字体支持和解决负号显示问题
rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体显示中文
rcParams['axes.unicode_minus'] = False  # 解决负号显示问题


class CommonUtils(object):
    def __init__(self):
        pass

    def get_year_week(self, current_date: datetime) -> str:
        """
        根据日期字符串获取年份和周数
        :param current_date:
        :return:
        """

        # 日期处理：主要是遇到周日时，往后推一天。因为早八点的一周 = 前一周的周日到本周的周六
        if current_date.weekday() == 6:
            current_date += timedelta(days=1)

        # 按照ISO 8601 标准获取年份和周数
        _, week, _ = current_date.isocalendar()

        return f"第{week:02d}周"

    def get_last_year_week_date(self, date_str: str) -> str:
        """
        获取上一年与指定日期在同一周且星期数相同的日期
        :param date_str:
        :return:
        """
        current_date = datetime.strptime(date_str, '%Y-%m-%d')
        # 获取当前日期的星期数
        weekday_this_year = current_date.weekday()
        # 获取上一年同一天的日期
        last_year_date = current_date.replace(year=current_date.year - 1)
        # 获取上一年同一天的星期数
        weekday_last_year = last_year_date.weekday()
        # 计算需要往前或往后推几天
        days_delta = (weekday_this_year - weekday_last_year) % 7
        # 计算上一年与指定日期在同一周且星期数相同的日期
        same_week_date_last_year = last_year_date + timedelta(days=days_delta)

        return same_week_date_last_year.strftime('%Y-%m-%d')

    def get_data_date_interval(self, specified_date: str, delta_days: int) -> Tuple[str, str]:
        """
        获取数据日期区间：根据指定日期获取数据日期区间
        :param specified_date: 指定日期
        :param delta_days: 区间长度
        :return:
        """
        end_date: str = (pd.to_datetime(specified_date) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        start_date: str = (pd.to_datetime(end_date) - pd.Timedelta(days=delta_days)).strftime('%Y-%m-%d')

        return start_date, end_date

    def generate_continous_data(self, city_list: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        生成连续数据：根据城市列表、开始日期、结束日期生成连续数据
        :param city_list: 城市列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return:
        """
        columns = ['城市', '数据日期']
        data_df = pd.DataFrame(columns=columns)
        date_range = [date_i for date_i in pd.date_range(start_date, end_date)]
        for city in city_list:
            city_df: pd.DataFrame = pd.DataFrame(data={'城市': [city] * len(date_range), '数据日期': date_range},
                                                 columns=columns)
            data_df = city_df if data_df.empty else pd.concat([data_df, city_df], ignore_index=True)

        return data_df

    def get_cn_holidays(self, years: List[int]) -> List[datetime.date]:
        """
        根据年份获取中国节假日日期
        :param years:
        :return:
        """
        cn_holidays = []
        for year in years:
            holiday_dates = holidays.country_holidays(country='CN', subdiv=None, years=year)
            cn_holidays.extend(holiday_dates.keys())

        return cn_holidays

    def gen_deal_trade_charts(self, newhouse_deal_df: pd.DataFrame,
                              secondhouse_deal_df: pd.DataFrame,
                              save_path: str,
                              date_flag: str) -> None:
        """
        生成周度图表：生成新房成交面积和二手房成交套数的周度图表
        :param newhouse_deal_df:
        :param secondhouse_deal_df:
        :param save_path:
        :return:
        """
        if date_flag == 'w':
            img_title = '图：30城新房近8周成交趋势（左）、20城二手房近8周成交趋势（右）'
        elif date_flag == 'm':
            img_title = '图：30城新房近6个月成交趋势（左）、20城二手房近6个月成交趋势（右）'
        else:
            print('输入错误，请重新输入！')
            return
        # 创建图表
        if date_flag == 'w':
            fig, axes = plt.subplots(1, 2, figsize=(12, 6), dpi=100)  # 2行1列布局
            fig.suptitle(img_title, fontsize=20, fontweight='bold')  # 设置标题并加粗

            # 新房成交面积柱状图
            axes[0].bar(newhouse_deal_df["周度数"], newhouse_deal_df["成交面积"], color='#008080')
            # axes[0].set_title("图：30城新房近8周成交趋势（左）", fontsize=20, fontweight='bold')  # 设置标题并加粗
            axes[0].set_ylabel("万平方米", fontsize=12, rotation=90)
            axes[0].tick_params(axis='x', rotation=45)  # 旋转x轴标签
            axes[0].spines['top'].set_visible(False)  # 去除上边框
            axes[0].spines['right'].set_visible(False)  # 去除右边框

            # 二手房成交套数柱状图
            axes[1].bar(secondhouse_deal_df["周度数"], secondhouse_deal_df["成交套数"], color='#ff7f50')
            # axes[1].set_title("图：20城二手房近8周成交趋势（右）", fontsize=20, fontweight='bold')  # 设置标题并加粗
            axes[1].set_ylabel("套", fontsize=12, rotation=90)
            axes[1].tick_params(axis='x', rotation=45)  # 旋转x轴标签
            axes[1].spines['top'].set_visible(False)  # 去除上边框
            axes[1].spines['right'].set_visible(False)  # 去除右边框

            # 调整布局并保存
            plt.tight_layout()
            plt.savefig(os.path.join(save_path, '新房二手房近8周成交趋势图.png'))  # 可选：保存图表
            plt.show()
            print('新房二手房近8周成交趋势图已生成！')
        elif date_flag == 'm':
            fig, axes = plt.subplots(1, 2, figsize=(12, 6), dpi=100)  # 2行1列布局
            fig.suptitle(img_title, fontsize=20, fontweight='bold')  # 设置标题并加粗

            # 新房成交面积柱状图
            axes[0].bar(newhouse_deal_df["月份"], newhouse_deal_df["成交面积"], color='#008080')
            # axes[0].set_title("图：30城新房近8周成交趋势（左）", fontsize=20, fontweight='bold')  # 设置标题并加粗
            axes[0].set_ylabel("万平方米", fontsize=12, rotation=90)
            axes[0].tick_params(axis='x', rotation=45)  # 旋转x轴标签
            axes[0].spines['top'].set_visible(False)  # 去除上边框
            axes[0].spines['right'].set_visible(False)  # 去除右边框

            # 二手房成交套数柱状图
            axes[1].bar(secondhouse_deal_df["月份"], secondhouse_deal_df["成交套数"], color='#ff7f50')
            # axes[1].set_title("图：20城二手房近8周成交趋势（右）", fontsize=20, fontweight='bold')  # 设置标题并加粗
            axes[1].set_ylabel("套", fontsize=12, rotation=90)
            axes[1].tick_params(axis='x', rotation=45)  # 旋转x轴标签
            axes[1].spines['top'].set_visible(False)  # 去除上边框
            axes[1].spines['right'].set_visible(False)  # 去除右边框

            # 调整布局并保存
            plt.tight_layout()
            plt.savefig(os.path.join(save_path, '新房二手房近6个月成交趋势图.png'))  # 可选：保存图表
            plt.show()
            print('新房二手房近6个月成交趋势图已生成！')


if __name__ == '__main__':
    common = CommonUtils()
    for date in common.get_cn_holidays([2023, 2024]):
        print(date)
