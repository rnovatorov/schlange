import setuptools

setuptools.setup(
    name="flockq",
    version="0.0.1",
    description="Persistent task queue built with flock",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    url="https://github.com/rnovatorov/python-flockq",
    author="Roman Novatorov",
    author_email="roman.novatorov@gmail.com",
    install_requires=[],
)
