from setuptools import find_packages, setup

setup(
    name="datawizard",
    version="0.0.1",
    author="Jorge Nachtigall",
    author_email="jlnvjunior@gmail.com",
    description="A tool for exporting Pandas dataframes to Redshift tables",
    packages=find_packages(),
    install_requires=["boto3>=1.34.109", "SQLAlchemy>=1.4.52", "pandas>=2.0.0"],
)
