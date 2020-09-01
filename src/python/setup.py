from setuptools import setup, find_packages

setup(name='data_lineage',
      version='0.5',
      description='Manage your data as it travels through its journey',
      url='https://github.grandlogic.com/projects/DATA/repos/data-lineage-controlplane',
      author='Sam Taha',
      author_email='sam@grandlogic.com',
      license='Apache 2.0',
      include_package_data=True,
      packages=find_packages('.', exclude=['test*',]),
      zip_safe=False)