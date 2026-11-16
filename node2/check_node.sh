#!/usr/bin/env bash
# Stage A read-only health check. Changes NOTHING on the system.
declare -A IP=( [node1]=10.108.165.201 [node2]=10.108.165.63 [node3]=10.108.165.243 [node4]=10.108.165.154 )
declare -A USR=( [node1]=vmware [node2]=pes2ug24cs537 [node3]=pes2ug24cs566 [node4]=thejus_07 )
NODES="node1 node2 node3 node4"
NF=0
pass(){ printf '  [PASS] %s\n' "$*"; }
fail(){ printf '  [FAIL] %s\n' "$*"; NF=$((NF+1)); }
info(){ printf '  [INFO] %s\n' "$*"; }

echo "Asking for sudo once (needed only to read zerotier-cli and ufw status)..."
sudo -v 2>/dev/null || true

echo "== 1. Identity =="
H=$(hostname)
ZTIPS=$(ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1 | grep '^10\.108\.165\.' | tr '\n' ' ')
ME=""
for n in $NODES; do echo " $ZTIPS " | grep -q " ${IP[$n]} " && ME=$n; done
info "hostname=$H  user=$(whoami)  ZeroTier IP(s)=${ZTIPS:-NONE}"
if [ -z "$ME" ]; then fail "no expected ZeroTier IP found on this VM (ZeroTier down or not joined?)"; ME=unknown
else pass "this VM is $ME by its ZeroTier IP ${IP[$ME]}"; fi
[ "$H" = "$ME" ] && pass "hostname matches ($H)" || fail "hostname is '$H', should be '$ME'"
[ "$(whoami)" = "${USR[$ME]}" ] && pass "user is ${USR[$ME]}" || info "user is $(whoami), expected ${USR[$ME]}"
grep -qs 'preserve_hostname: *false' /etc/cloud/cloud.cfg && info "cloud-init may reset hostname on reboot (preserve_hostname: false)"
grep -qs 'manage_etc_hosts: *true' /etc/cloud/cloud.cfg && info "cloud-init may rewrite /etc/hosts on reboot (manage_etc_hosts: true)"

echo "== 2. /etc/hosts and name resolution =="
if grep -q '^127\.0\.1\.1' /etc/hosts; then fail "127.0.1.1 line present: $(grep '^127\.0\.1\.1' /etc/hosts)"; else pass "no 127.0.1.1 line"; fi
for n in $NODES; do
  got=$(getent hosts "$n" | awk '{print $1}' | head -1)
  [ "$got" = "${IP[$n]}" ] && pass "$n -> $got" || fail "$n resolves to '${got:-nothing}', expected ${IP[$n]}"
done
grep -nE 'node[1-4]|10\.108\.165\.' /etc/hosts | sed 's/^/  [INFO] hosts line /'

echo "== 3. Connectivity (ping by IP, avg ms) =="
for n in $NODES; do
  [ "$n" = "$ME" ] && continue
  out=$(ping -c 3 -W 2 -q "${IP[$n]}" 2>/dev/null)
  if [ $? -eq 0 ]; then pass "ping $n  avg $(echo "$out" | awk -F/ '/rtt|round-trip/{print $5}') ms"
  else fail "ping $n (${IP[$n]}) failed"; fi
done

echo "== 4. ZeroTier =="
if command -v zerotier-cli >/dev/null; then
  sudo zerotier-cli info | sed 's/^/  [INFO] /'
  sudo zerotier-cli listnetworks | sed 's/^/  [INFO] /'
  info "peers (LEAF rows = other VMs; want DIRECT, not RELAY):"
  sudo zerotier-cli peers | grep -E 'LEAF|<ztaddr>' | sed 's/^/         /'
else fail "zerotier-cli not installed"; fi

echo "== 5. Java / Python =="
ROLE=$ME; [ "$ROLE" = unknown ] && ROLE=$H
SPARKNODE=no; case "$ROLE" in node1|node2|node3) SPARKNODE=yes;; node4) ;; *) [ -n "$SPARK_HOME" ] && SPARKNODE=yes;; esac
JV=$(java -version 2>&1 | grep -m1 -i version)
if [ "$SPARKNODE" = yes ]; then
  echo "$JV" | grep -q '"11\.' && pass "java: $JV" || fail "java should be 11, got: ${JV:-not installed}"
else info "java (not needed on node4): ${JV:-not installed}"; fi
info "python3 -> $(python3 --version 2>&1)"
if [ "$SPARKNODE" = yes ]; then
  P12=$(python3.12 --version 2>&1)
  echo "$P12" | grep -q '3\.12' && pass "python3.12 present: $P12 ($(command -v python3.12))" || fail "python3.12 missing (Spark nodes must all use 3.12)"
fi

echo "== 6. Environment variables (from ~/.bashrc) =="
if [ "$SPARKNODE" = yes ]; then
  for v in JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR SPARK_HOME HADOOP_USER_NAME PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON; do
    val="${!v}"
    if [ -z "$val" ]; then fail "$v is EMPTY"; continue; fi
    case $v in *HOME|*DIR) [ -d "$val" ] && pass "$v=$val" || fail "$v=$val (directory does not exist)";;
      HADOOP_USER_NAME) [ "$val" = pes2ug24cs537 ] && pass "$v=$val" || fail "$v=$val, should be pes2ug24cs537";;
      *) pass "$v=$val";; esac
  done
  info "hadoop: $(hadoop version 2>/dev/null | head -1)"
  info "spark:  $(head -1 "$SPARK_HOME/RELEASE" 2>/dev/null)"
  ls "$SPARK_HOME"/jars/iceberg-spark-runtime-3.5_2.12-*.jar >/dev/null 2>&1 \
    && pass "iceberg jar: $(basename "$SPARK_HOME"/jars/iceberg-spark-runtime-3.5_2.12-*.jar)" \
    || fail "iceberg-spark-runtime-3.5_2.12 jar not in \$SPARK_HOME/jars"
  for f in core-site.xml hdfs-site.xml; do
    [ -f "$HADOOP_CONF_DIR/$f" ] && info "$f: $(grep -oE '<(name|value)>[^<]*' "$HADOOP_CONF_DIR/$f" | sed 's/<[a-z]*>//' | paste -sd' ' | cut -c1-300)" || fail "$HADOOP_CONF_DIR/$f missing"
  done
  for f in spark-env.sh spark-defaults.conf; do
    [ -f "$SPARK_HOME/conf/$f" ] && info "$f: $(grep -vE '^\s*(#|$)' "$SPARK_HOME/conf/$f" | paste -sd'|' | cut -c1-300)" || info "$SPARK_HOME/conf/$f not present yet"
  done
else
  [ -x ~/venv/bin/python ] && info "venv python: $(~/venv/bin/python --version 2>&1)" || fail "~/venv missing"
  ~/venv/bin/python -c 'import fastapi,uvicorn,pandas,pyarrow' 2>/dev/null && pass "venv has fastapi uvicorn pandas pyarrow" || fail "venv missing one of fastapi/uvicorn/pandas/pyarrow"
  for c in rsync ssh; do command -v $c >/dev/null && pass "$c installed" || fail "$c not installed"; done
  [ -d /serving ] && info "/serving owner: $(stat -c %U /serving)" || fail "/serving does not exist"
  ls ~/.ssh/id_*.pub >/dev/null 2>&1 && pass "ssh key exists" || fail "no ssh key in ~/.ssh"
fi

echo "== 7. Misc =="
[ -d ~/search ] && pass "~/search exists" || fail "~/search missing"
info "sshd: $(systemctl is-active ssh 2>/dev/null || echo unknown)   ufw: $(sudo ufw status 2>/dev/null | head -1)"
info "disk free on ~: $(df -h ~ | awk 'NR==2{print $4}')   RAM: $(free -h | awk '/Mem:/{print $2" total, "$7" avail"}')   CPUs: $(nproc)"
command -v jps >/dev/null && info "jps: $(jps 2>/dev/null | grep -v Jps | awk '{print $2}' | paste -sd' ')"

echo "== SUMMARY: $ME has $NF FAIL(s) =="
