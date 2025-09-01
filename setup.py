import setuptools

setuptools.setup(
    name="schlange-queue",
    version="0.0.1",
    description="Lightweight, persistent, single-node task queue",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    package_data={"schlange": ["sqlite/migrations/*.sql"]},
    url="https://github.com/rnovatorov/schlange",
    author="Roman Novatorov",
    author_email="roman.novatorov@gmail.com",
    install_requires=[],
)
