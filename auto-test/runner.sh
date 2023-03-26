#! /bin/bash

data=`sed -r -n '/qa_/s/:[\\n,n,\n]?//;/qa_/p' ../config.yaml | awk -F 'qa_' '{print $2}'`
arr_data=()
for i in $data; do
  arr_data+=($i)
done
data_nums=${#arr_data[*]}
num=1

######################################################################################################################

cases_data1=`ls test_cp_* | awk -F '.' '{print $1}'`
arr_cases1=()
for c in $cases_data1; do
  arr_cases1+=($c)
done
cases_nums1=${#arr_cases1[*]}

######################################################################################################################

cases_data2=`ls test_dev_* | awk -F '.' '{print $1}'`
arr_cases2=()
for c in $cases_data2; do
  arr_cases2+=($c)
done
cases_nums2=${#arr_cases2[*]}

nums=1


function sources() {
    case $1 in
      'mongodb')
                echo $1
                read -p 'Please enter the test cases to run:' cases_name
                if [[ $cases_name == '' ]]; then
                    cases_name='null'
                    cases $cases_name $1 $2
                else
                    cases $cases_name $1 $2
                fi;;
      *)
        for d in $data; do
          case $1 in
            ${d})
                 read -p "Please enter the test cases to run:" cases_name
                 if [[ $cases_name == '' ]]; then
                    cases_name='null'
                    cases $cases_name $1 $2
                 else
                    cases $cases_name $1 $2
                 fi
                 break;;
            *)
              echo $d
              if [[ $num -eq $data_nums ]]; then
                  read -p "The data source does not exist, please re-enter:" source
                  num=1
                  if [[ $source == '' ]]; then
                    source='null'
                    sources $source $2
                  else
                    sources $source $2
                  fi
              else
                  echo "----------------------------------"
              fi;;
          esac
          ((num++))
        done;;
    esac
}


function cases() {
    case $1 in
      'all')
            case $2 in
              'mongodb')
                        case $3 in
                          'merge')
                                  cases_data=$cases_data1
                                  cases_nums=$cases_nums1;;
                          'sync')
                                 cases_data=$cases_data2
                                 cases_nums=$cases_nums2;;
                        esac
                        for ca in $cases_data; do
                          echo $ca
                          python runner.py --case $ca.py --bench 123
                        done;;
              *)
                case $3 in
                  'merge')
                          cases_data=$cases_data1
                          cases_nums=$cases_nums1;;
                  'sync')
                         cases_data=$cases_data2
                         cases_nums=$cases_nums2;;
                esac
                for ca in $cases_data; do
                  echo $ca
                  python runner.py --case $ca.py --source qa_$2 --smart_cdc --bench 123
                done;;
            esac;;
      *)
        case $3 in
          'merge')
                  cases_data=$cases_data1
                  cases_nums=$cases_nums1;;
          'sync')
                 cases_data=$cases_data2
                 cases_nums=$cases_nums2;;
        esac
        for ca in $cases_data; do
          echo $ca
          case $1 in
            ${ca})
                  case $2 in
                    'mongodb')
                              python runner.py --case $1.py --bench 123
                              break;;
                    *)
                      python runner.py --case $1.py --source qa_$2 --smart_cdc --bench 123
                      break;;
                  esac;;
            *)
              if [[ $nums -eq $cases_nums ]]; then
                  read -p "For test cases that do not exist, please re-enter:" cases_name
                  nums=1
                  if [[ $cases_name == '' ]]; then
                    cases_name='null'
                    cases $cases_name $2 $3
                  else
                    cases $cases_name $2 $3
                  fi
              else
                  echo "----------------------------------"
              fi;;
          esac
          ((nums++))
        done;;
    esac
}

function type() {

  case $1 in
    sync|merge)
          read -p 'Please enter which data source you want to source from:' source
          if [[ $source == '' ]]; then
            source='null'
            sources $source $1
          else
            sources $source $1
          fi;;
    *)
      read -p 'No cases of this type exists. Please re-enter:' job_type
      type $job_type
  esac
}


read -p 'Please enter the type of cases(merge or sync):' job_type
type $job_type