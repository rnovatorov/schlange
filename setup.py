import setuptools

setuptools.setup(
    name="schlange-queue",
    version="0.1.2",
    description="Lightweight, persistent, single-node task queue with at-least-once delivery guarantee",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    package_data={
        "schlange": [
            "services/leases/sqlite/migrations/*.sql",
            "services/schedules/sqlite/migrations/*.sql",
            "services/tasks/sqlite/migrations/*.sql",
        ],
    },
    url="https://github.com/rnovatorov/schlange",
    author="Roman Novatorov",
    author_email="roman.novatorov@gmail.com",
    install_requires=[],
)
