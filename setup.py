from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)

install_requires = (
    "aiohttp==3.7.2",
    "neuro_auth_client==19.10.5",
    "platform_config_client==21.1.4",
    "neuromation==20.12.7",
    "aioelasticsearch==0.7.0",
    "aiodocker==0.19.1",
    "docker-image-py==0.1.10",
    "trafaret==2.1.0",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
    "aiobotocore==1.1.2",
    "urllib3>=1.20,<1.26",  # botocore requirements
)

setup(
    name="platform-monitoring",
    url="https://github.com/neuromation/platform-monitoring",
    use_scm_version={
        "git_describe_command": "git describe --dirty --tags --long --match v*.*.*",
    },
    packages=find_packages(),
    setup_requires=setup_requires,
    install_requires=install_requires,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": ["platform-monitoring=platform_monitoring.api:main"]
    },
    zip_safe=False,
)
