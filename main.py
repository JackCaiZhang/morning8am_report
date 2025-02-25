import sys
from datetime import datetime, timedelta

from Report_8am_morning import Report8AmMorning


def task_exec(config_path: str):
    date_flag: int | None = None
    time_flag: str | None = None
    time_message: str = '请输入时间维度标志（w--周；m--月）：'
    period_message: str = '请输入时间周期标志（1--本周期；2--其他周期）：'
    time_flag = input(time_message).lower()
    if time_flag == 'w':
        date_flag = int(input(period_message))
        if date_flag == 1:
            # 用当天日期作为报告日期
            report_date = datetime.now().strftime('%Y-%m-%d')
            report: Report8AmMorning = Report8AmMorning(config_path, report_date, time_flag='w')
            report.data_statistics()
        elif date_flag == 2:
            # 输入报告日期
            report_date = input('请输入报告日期-某个周日（yyyy-mm-dd）：')
            report: Report8AmMorning = Report8AmMorning(config_path, report_date, time_flag='w')
            report.data_statistics()
        else:
            print('输入错误，请重新输入！')
            sys.exit(0)
    elif time_flag =='m':
        date_flag = int(input(period_message))
        if date_flag == 1:
            # 用报告数据所在月份最后一天作为报告日期
            report_date = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
            report: Report8AmMorning = Report8AmMorning(config_path, report_date, time_flag='m')
            report.data_statistics()
        elif date_flag == 2:
            # 输入报告日期
            report_date = input('请输入报告日期-某个月最后一天（yyyy-mm-dd）：')
            report: Report8AmMorning = Report8AmMorning(config_path, report_date, time_flag='m')
            report.data_statistics()
        else:
            print('输入错误，请重新输入！')
            sys.exit(0)
    else:
        print('输入错误，请重新输入！')
        sys.exit(0)


if __name__ == '__main__':
    config_path: str = r'data_files/config_file.xlsx'
    task_exec(config_path)
