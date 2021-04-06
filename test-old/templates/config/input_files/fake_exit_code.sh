set -x
echo "================= CMSRUN starting ===================="
cmsRun -j FrameworkJobReport.xml -p PSet.py
echo "================= CMSRUN finished ===================="

#Prepare exit codes
#System EXIT CODES
EXIT_CODES=()
for i in {0..256}
  do
    EXIT_CODES=("${EXIT_CODES[@]}" $i)
  done

#CmsRun Exit codes
for i in {7000..7030}
  do
    EXIT_CODES=("${EXIT_CODES[@]}" $i)
  done
for i in {8000..8030}
  do
    EXIT_CODES=("${EXIT_CODES[@]}" $i)
  done

exitCode=${EXIT_CODES[$1]}
if [ -z $exitCode ]; then
  exitCode=0
fi
echo "POSSIBLE_EXIT_CODES=${EXIT_CODES[*]}"
echo "EXIT_CODE_TEST=$exitCode"

exitMessage="This is a test to see if I can pass exit code ANY to CRAB."
errorType=""

if [ -e FrameworkJobReport.xml ]
then
    cat << EOF > FrameworkJobReport.xml.tmp
<FrameworkJobReport>
<FrameworkError ExitStatus="$exitCode" Type="$errorType" >
$exitMessage
</FrameworkError>
EOF
    tail -n+2 FrameworkJobReport.xml >> FrameworkJobReport.xml.tmp
    mv FrameworkJobReport.xml.tmp FrameworkJobReport.xml
else
    cat << EOF > FrameworkJobReport.xml
<FrameworkJobReport>
<FrameworkError ExitStatus="$exitCode" Type="$errorType" >
$exitMessage
</FrameworkError>
</FrameworkJobReport>
EOF
fi
set +x
