import itertools
import sys
import typing
from multiprocessing import Pool
import subprocess


class RunParameters(typing.NamedTuple):
    wfName: str
    type: str
    percent: float
    home_dir: str


def run_one(params: RunParameters):
    print(f"starting params {repr(params)}")
    res = subprocess.run([
        "java", "-classpath",
        f"{params.home_dir}/ba/worksim/target/classes:{params.home_dir}/.m2/repository/org/jdom/jdom2/2.0.6.1/jdom2-2.0.6.1.jar:{params.home_dir}/.m2/repository/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar:{params.home_dir}/.m2/repository/com/jayway/jsonpath/json-path/2.7.0/json-path-2.7.0.jar:{params.home_dir}/.m2/repository/net/minidev/json-smart/2.4.7/json-smart-2.4.7.jar:{params.home_dir}/.m2/repository/net/minidev/accessors-smart/2.4.7/accessors-smart-2.4.7.jar:{params.home_dir}/.m2/repository/org/ow2/asm/asm/9.1/asm-9.1.jar:{params.home_dir}/.m2/repository/org/slf4j/slf4j-api/1.7.33/slf4j-api-1.7.33.jar:{params.home_dir}/.m2/repository/com/guicedee/services/sl4j/1.0.13.5/sl4j-1.0.13.5.jar:{params.home_dir}/.m2/repository/com/guicedee/services/log4j-core/1.0.13.5/log4j-core-1.0.13.5.jar",
        "examples.org.workflowsim.examples.WorkflowSimBasicExample1",
        params.wfName, params.type, str(params.percent)], check=True, stdout=subprocess.DEVNULL
    )
    print(res.args)
    print(f"done with params {repr(params)}")
    return res


def run_many(wfs, types, percents, home_dir):
    params: typing.List[RunParameters] = [RunParameters(wf, t, p, home_dir) for wf, t, p in
                                          itertools.product(wfs, types, percents)]
    with Pool() as pool:
        print("starting")
        print(f"running {len(params)} configurations")
        results = pool.map(run_one, params)
        print("done")


def main():
    return run_many(["chipseq", "viralrecon", "eager", "methylseq"], ["normal", "exponential"],
                    [0.00, 0.05, 0.10, 0.15, 0.20], sys.argv[1])
    # print("test")
    # t = run_one(RunParameters("eager", "normal", 0.15))
    # print(t)
    # return t


if __name__ == '__main__':
    main()
