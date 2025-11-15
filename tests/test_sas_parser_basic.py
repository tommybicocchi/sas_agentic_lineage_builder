# tests/test_sas_parser_basic.py

from utils import sas_parser


def run_tests():
    """
    Test base su utils.sas_parser.parse_dependencies.
    """

    print("=== Test sas_parser (basic): INIZIO ===")

    code = """
    %macro mymacro(param);
      %put &=param;
    %mend;

    %mymacro(123);

    data work.out_ds;
      set work.in_ds1 work.in_ds2;
    run;

    proc sort data=work.out_ds out=work.sorted_ds;
      by id;
    run;

    %include "/path/to/include1.sas";
    %include 'other/include2.sas';
    """

    deps = sas_parser.parse_dependencies(code)
    print("Deps:", deps)

    assert "work.in_ds1" in deps["input_datasets"]
    assert "work.in_ds2" in deps["input_datasets"]
    assert "work.out_ds" in deps["output_datasets"]
    assert "work.sorted_ds" in deps["output_datasets"]  # via PROC out=
    assert "mymacro" in deps["macro_calls"]
    assert "/path/to/include1.sas" in deps["include_files"]
    assert "other/include2.sas" in deps["include_files"]
    assert "sort" in deps["proc_types"]
    assert deps["has_dynamic_deps"] is True  # a causa di &param nel macro body

    print("=== Test sas_parser (basic): TUTTO OK ✅ ===")


if __name__ == "__main__":
    run_tests()