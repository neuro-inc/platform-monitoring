from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.6.2",
    'dataclasses==0.6; python_version<"3.7"',
    "yarl==1.3.0",
    "neuro_auth_client==1.0.10",
    "neuromation==19.7.4",
    "async-exit-stack==1.0.1",  # backport from 3.7 stdlib
    "aioelasticsearch==0.5.2",
    "aiodocker==0.17.0",
    "docker-image-py==0.1.10",
    "trafaret==1.2.0",
    "platform-logging==0.3",
)

setup(
    name="platform-monitoring",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-monitoring",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        "console_scripts": ["platform-monitoring=platform_monitoring.api:main"]
    },
    zip_safe=False,
)
