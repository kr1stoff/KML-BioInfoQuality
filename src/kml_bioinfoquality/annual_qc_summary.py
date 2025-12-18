import click
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@click.command()
@click.option('--input-file', required=True, help='输入特定项目QC结果文件的列表, 每行一条绝对路径')
# todo 后面按需添加项目
@click.option('--project-name', default='tcr', show_default=True, type=click.Choice(['tcr', 'lvis']), help='项目名称')
@click.option('--output-file', required=True, help='输出年度QC统计结果的Excel文件')
@click.help_option('--help', help='显示帮助信息')
def main(input_file: str, project_name: str, output_file: str):
    """年度QC统计"""
    logging.debug(f"输入文件: {input_file}")
    logging.debug(f"项目名称: {project_name}")
    logging.debug(f"输出文件: {output_file}")
    logging.info('开始统计年度QC')
    if project_name == 'tcr':
        base_name, q30_name = 'Total_bases', 'Q30'
    elif project_name == 'lvis':
        base_name, q30_name = 'RawBases', 'CleanQ30'
    qc_summary(input_file, output_file, base_name, q30_name)
    logging.info('年度QC统计完成')


def qc_summary(input_file: str, output_file: str, base_name: str, q30_name: str):
    """年度QC统计"""
    dfs = []
    stats_files = open(input_file).readlines()
    for sf in stats_files:
        sf = sf.strip()
        df = pd.read_csv(sf, sep='\t')
        # 去除掉POS和NTC
        df = df[~df['Sample'].str.contains('POS|NTC', na=False, regex=True, case=False)]
        dfs.append(df)
    # 合并所有数据框
    final_df = pd.concat(dfs)
    # 按照样本名和总碱基数排序，取每个样本的最大值
    final_df_sorted = final_df.sort_values(by=['Sample', base_name], ascending=[True, False])
    final_df_unique = final_df_sorted.drop_duplicates(subset=['Sample'], keep='first')
    bases_stats = final_df_unique[base_name].quantile([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]).astype(int)
    q30_stats = final_df_unique[q30_name].quantile([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1])
    pd.DataFrame({'TotalBases': bases_stats, 'Q30': q30_stats}).to_excel(output_file)


if __name__ == '__main__':
    main()
