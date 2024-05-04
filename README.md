# qt-etl
> python金工数据缓存仓库，依托Apache pyarrow库打造具有一流的集成使用NumPy、pandas和内置的Python对象。它们基于C++ Arrow的实现， 使大数据系统能够快速存储、处理和移动内存式。该工程具备快速etl数据抽象的能力，使用者可根据业务需要定期更新和维护本身所需的业务数据模型，以供其他应用和平台工程化使用。
### 工程结构
```
qt-etl
    ├── README.md                                        # 说明文档
    ├── conf.ini.example                            
    ├── environment.yaml                                 # 依赖安装包
    └── qt_etl                                           # qt_etl应用主目录
        ├── __init__.py
        ├── config.py                                    # qt_etl应用配置
        ├── constants.py                                 # qt_etl应用通用常量
        ├── entity                                       # 存储etl实体
        │   ├── __init__.py 
        │   ├── entity_base.py                           # 存储etl实体基类
        │   ├── factor                                   # 多级因子模型目录
        │   │   ├── __init__.py
        │   │   ├── barra_cne6_level1.py
        │   │   ├── barra_cne6_level2.py
        │   │   ├── barra_cne6_level2_mean.py
        │   │   ├── barra_cne6_level2_std.py
        │   │   ├── barra_cne6_level3.py
        │   │   ├── barra_cne6_level3_mean.py
        │   │   ├── barra_cne6_level3_std.py
        │   │   ├── barra_factor_return_cne6.py
        │   │   ├── factor.py
        │   │   └── financial_indicator.py
        │   ├── fields.py                                # etl字段映射关系、注释
        │   ├── instruments                              # 基本信息模型目录
        │   │   ├── __init__.py
        │   │   ├── benchmark_info.py
        │   │   ├── bond_info.py
        │   │   ├── fund_info.py
        │   │   ├── instrument.py
        │   │   ├── party_rating_info.py
        │   │   ├── stock_index_info.py
        │   │   ├── stock_info.py
        │   │   └── zg_sec_info.py
        │   ├── market_data                               # 市场数据模型目录
        │   │   ├── __init__.py
        │   │   ├── benchmark_mkt_data.py
        │   │   ├── bond_market_data.py
        │   │   ├── bond_mkt_data.py
        │   │   ├── cnbd_sample_data.py
        │   │   ├── custom_bond_mkt_data.py
        │   │   ├── fund_market_data.py
        │   │   ├── fund_mkt_data.py
        │   │   ├── index_rate.py
        │   │   ├── industry_classification_mkt_data.py
        │   │   ├── market_data.py
        │   │   ├── qt_calendar.py
        │   │   └── stock_mkt_data.py
        │   ├── portfolio                                 # 持仓数据模型目录
        │   │   ├── __init__.py
        │   │   ├── bond_fund_portfolio.py
        │   │   ├── bond_position.py
        │   │   ├── comb_position.py
        │   │   ├── down_relation_info.py
        │   │   ├── portfolio.py
        │   │   ├── repo_position.py
        │   │   ├── stock_fund_portfolio.py
        │   │   └── stock_index_portfolio.py
        │   └── trade                                     # 交易数据模型目录
        │       ├── __init__.py
        │       ├── bond_trade.py
        │       ├── bond_trade_penetrate.py
        │       ├── stock_trade.py
        │       ├── stock_trade_penetrate.py
        │       └── trade.py
        ├── err_code.py                                    # etl业务异常码定义
        ├── script                                         # etl脚本存放
        │   ├── __init__.py
        │   ├── batch_etl_run.py                           # 手动运行etl-异步
        │   ├── etl_record.py                              # etl元信息导出、记录读取
        │   ├── etl_run.py                                 # 手动运行etl-同步
        │   ├── etl_schema.py
        │   ├── etl_table.py
        │   └── sync_not_info_etl.py                       # 手动运行etl-同步-不包含常用INFO表
        └── utils.py                                       # etl常用工具模块

```

## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it
easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file)
  or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line)
  or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://gitlab.iquantex.com/qt-quant/qt-etl.git
git branch -M master
git push -uf origin master
```

## Integrate with your tools

- [ ] [Set up project integrations](https://gitlab.iquantex.com/qt-quant/qt-etl/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Automatically merge when pipeline succeeds](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing(SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or
feel free to structure it however you want - this is just a starting point!). Thank you
to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README

Every project is different, so consider which of these sections apply to yours. The sections used in the
template are suggestions for most open source projects. Also keep in mind that while a README can be too
long and detailed, too long is better than too short. If you think your README is too long, consider
utilizing another form of documentation rather than cutting out information.

## Name

Choose a self-explaining name for your project.

## Description

Let people know what your project can do specifically. Provide context and add a link to any reference
visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If
there are alternatives to your project, this is a good place to list differentiating factors.

## Badges

On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are
passing for the project. You can use Shields to add some to your README. Many services also have
instructions for adding a badge.

## Visuals

Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll
frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a
more sophisticated method.

## Installation

Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or
Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like
more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as
quickly as possible. If it only runs in a specific context like a particular programming language version or
operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage

Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest
example of usage that you can demonstrate, while providing links to more sophisticated examples if they are
too long to reasonably include in the README.

## Support

Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an
email address, etc.

## Roadmap

If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing

State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get
started. Perhaps there is a script that they should run or some environment variables that they need to set.
Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality
and reduce the likelihood that the changes inadvertently break something. Having instructions for running
tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in
a browser.

## Authors and acknowledgment

Show your appreciation to those who have contributed to the project.

## License

For open source projects, say how it is licensed.

## Project status

If you have run out of energy or time for your project, put a note at the top of the README saying that
development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to
step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request
for maintainers.
