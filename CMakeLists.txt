cmake_minimum_required(VERSION 3.10)
project(Advanced_DBMS_Implementation C)

set(CMAKE_C_STANDARD 11)

include_directories(.)
add_executable(Advanced_DBMS_Implementation
#[[        UnitTesting/Unity-master/Testing/example_1/src/ProductionCode.c
        UnitTesting/Unity-master/Testing/example_1/src/ProductionCode.h
        UnitTesting/Unity-master/Testing/example_1/src/ProductionCode2.c
        UnitTesting/Unity-master/Testing/example_1/src/ProductionCode2.h
        UnitTesting/Unity-master/Testing/example_1/test/test_runners/TestProductionCode2_Runner.c
        UnitTesting/Unity-master/Testing/example_1/test/test_runners/TestProductionCode_Runner.c
        UnitTesting/Unity-master/Testing/example_1/test/TestProductionCode.c
        UnitTesting/Unity-master/Testing/example_1/test/TestProductionCode2.c
        UnitTesting/Unity-master/Testing/example_2/src/ProductionCode.c
        UnitTesting/Unity-master/Testing/example_2/src/ProductionCode.h
        UnitTesting/Unity-master/Testing/example_2/src/ProductionCode2.c
        UnitTesting/Unity-master/Testing/example_2/src/ProductionCode2.h
        UnitTesting/Unity-master/Testing/example_2/test/test_runners/all_tests.c
        UnitTesting/Unity-master/Testing/example_2/test/test_runners/TestProductionCode2_Runner.c
        UnitTesting/Unity-master/Testing/example_2/test/test_runners/TestProductionCode_Runner.c
        UnitTesting/Unity-master/Testing/example_2/test/TestProductionCode.c
        UnitTesting/Unity-master/Testing/example_2/test/TestProductionCode2.c]]
        #[[UnitTesting/Unity-master/Testing/RadixHashJoin_Test/helper/UnityHelper.c
        UnitTesting/Unity-master/Testing/RadixHashJoin_Test/helper/UnityHelper.h
        UnitTesting/Unity-master/Testing/RadixHashJoin_Test/src/ProductionCode.c
        UnitTesting/Unity-master/Testing/RadixHashJoin_Test/src/ProductionCode.h
        UnitTesting/Unity-master/Testing/RadixHashJoin_Test/src/ProductionCode2.c
        UnitTesting/Unity-master/Testing/RadixHashJoin_Test/src/ProductionCode2.h
        UnitTesting/Unity-master/Testing/RadixHashJoin_Test/test/TestProductionCode.c
        UnitTesting/Unity-master/Testing/RadixHashJoin_Test/test/TestProductionCode2.c
        UnitTesting/Unity-master/Testing/unity_config.h
        UnitTesting/Unity-master/extras/fixture/src/unity_fixture.c
        UnitTesting/Unity-master/extras/fixture/src/unity_fixture.h
        UnitTesting/Unity-master/extras/fixture/src/unity_fixture_internals.h
        UnitTesting/Unity-master/extras/fixture/src/unity_fixture_malloc_overrides.h
        UnitTesting/Unity-master/extras/fixture/test/main/AllTests.c
        UnitTesting/Unity-master/extras/fixture/test/template_fixture_tests.c
        UnitTesting/Unity-master/extras/fixture/test/unity_fixture_Test.c
         UnitTesting/Unity-master/extras/fixture/test/unity_fixture_TestRunner.c
         UnitTesting/Unity-master/extras/fixture/test/unity_output_Spy.c
         UnitTesting/Unity-master/extras/fixture/test/unity_output_Spy.h
         UnitTesting/Unity-master/src/unity.c
         UnitTesting/Unity-master/src/unity.h
         UnitTesting/Unity-master/src/unity_internals.h
         UnitTesting/Unity-master/test/expectdata/testsample_cmd.c
         UnitTesting/Unity-master/test/expectdata/testsample_def.c
         UnitTesting/Unity-master/test/expectdata/testsample_head1.c
         UnitTesting/Unity-master/test/expectdata/testsample_head1.h
         UnitTesting/Unity-master/test/expectdata/testsample_mock_cmd.c
         UnitTesting/Unity-master/test/expectdata/testsample_mock_def.c
         UnitTesting/Unity-master/test/expectdata/testsample_mock_head1.c
         UnitTesting/Unity-master/test/expectdata/testsample_mock_head1.h
         UnitTesting/Unity-master/test/expectdata/testsample_mock_new1.c
         UnitTesting/Unity-master/test/expectdata/testsample_mock_2.c
         #[[UnitTesting/Unity-master/test/expectdata/testsample_mock_param.c
         UnitTesting/Unity-master/test/expectdata/testsample_mock_run1.c
         UnitTesting/Unity-master/test/expectdata/testsample_mock_run2.c
         UnitTesting/Unity-master/test/expectdata/testsample_mock_yaml.c
         UnitTesting/Unity-master/test/expectdata/testsample_new1.c
         UnitTesting/Unity-master/test/expectdata/testsample_new2.c
         UnitTesting/Unity-master/test/expectdata/testsample_param.c
         UnitTesting/Unity-master/test/expectdata/testsample_run1.c
         UnitTesting/Unity-master/test/expectdata/testsample_run2.c
         UnitTesting/Unity-master/test/expectdata/testsample_yaml.c
         UnitTesting/Unity-master/test/testdata/CException.h
         UnitTesting/Unity-master/test/testdata/cmock.h
         UnitTesting/Unity-master/test/testdata/Defs.h
         UnitTesting/Unity-master/test/testdata/mockMock.h
         UnitTesting/Unity-master/test/testdata/testRunnerGenerator.c
         UnitTesting/Unity-master/test/testdata/testRunnerGeneratorSmall.c
         UnitTesting/Unity-master/test/testdata/testRunnerGeneratorWithMocks.c
         UnitTesting/Unity-master/test/tests/testparameterized.c
         UnitTesting/Unity-master/test/tests/testunity.c]]
        main.c
        radixHashJoin.c
        radixHashJoin.h
        supportFunctions.c
        supportFunctions.h)

target_link_libraries(Advanced_DBMS_Implementation m)
