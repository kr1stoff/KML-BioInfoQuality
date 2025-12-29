import click
from subprocess import run
import re
from pathlib import Path
import pandas as pd
import logging

from src.config.software import OBSUTIL


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--input-file',
    required=True,
    help='输入特定项目结果目录路径的文件, 逗号分隔. 逗号前为华为云存储OBS上备份文件夹路径, 逗号后为本地文件夹路径'
)
@click.option('--output-file', required=True, help='输出结果目录')
@click.help_option('--help', help='显示帮助信息')
def main(input_file, output_file):
    logging.debug(f"输入文件: {input_file}")
    logging.debug(f"输出文件: {output_file}")
    logging.info(f'开始检查备份文件夹: {input_file}')

    results = []
    with open(input_file) as f:
        for line in f:
            obs_path, local_path = line.strip().split(',')
            obs_dir_name = Path(obs_path).name
            local_dir_name = Path(local_path).name
            # 文件夹名一致检查
            if obs_dir_name != local_dir_name:
                raise Exception(f'OBS 文件夹名 {obs_dir_name} 与本地文件夹名 {local_dir_name} 不匹配')
            # OBS 上的文件夹大小和文件数量
            run_output = run(f'{OBSUTIL} ls {obs_path} -limit=-1 | tail', shell=True, capture_output=True, text=True)
            try:
                res = re.findall(
                    r'Total size of prefix .*: (.*?)GB\nFolder number: \d+\nFile number: (\d+)', run_output.stdout)
                obs_dire_size = float(res[0][0])
                obs_file_num = int(res[0][1])
            except IndexError:
                raise IndexError(f'无法查询OBS文件信息. \n{obs_path}')
            # 本地文件数量
            run_output = run(f'tree {local_path} | tail', shell=True, capture_output=True, text=True)
            try:
                res = re.findall(r'\d+ directories, (\d+) files', run_output.stdout)
                local_file_num = int(res[0])
            except IndexError:
                raise IndexError(f'无法查询本地文件信息. \n{local_path}')
            # 本地文件夹大小
            run_output = run(f'du -s {local_path}', shell=True, capture_output=True, text=True)
            try:
                local_dir_size = round(int(run_output.stdout.split('\t')[0]) / (1024**2), 2)
            except IndexError:
                raise IndexError(f'无法查询本地文件夹大小. \n{local_path}')
            # 判断OBS和本地一致
            if (abs(obs_dire_size - local_dir_size) < 0.1) and (obs_file_num == local_file_num):
                check = 'PASS'
            else:
                check = 'FAIL'
            results.append([obs_path, local_path, obs_dire_size, local_dir_size, obs_file_num, local_file_num, check])
            logging.info(f'{obs_path} {local_path} {obs_dire_size} {local_dir_size} {obs_file_num} {local_file_num} {check}')
    # 生成Excel
    df = pd.DataFrame(results, columns=['OBS路径', '本地路径', 'OBS文件夹大小(GB)', '本地文件夹大小(GB)', 'OBS文件数量', '本地文件数量', '检查结果'])
    df.to_excel(output_file, index=False)

    logging.info(f'检查备份文件夹完成: {input_file}')


if __name__ == '__main__':
    main()
