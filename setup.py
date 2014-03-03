from distutils.core import setup

setup(
    name='Glider Singleton Publishing Service',
    version='1.0',
    author='Michael Lindemuth',
    author_email='mlindemu@usf.edu',
    packages=['glider_singleton_publishing_service'],
    scripts=[
        'glider_singleton_publishing_service/gsps.py',
    ]
)
