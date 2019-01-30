use strict;
use warnings;

use B qw(svref_2object SVf_IOK SVf_NOK SVf_POK SVf_IVisUV);
use Test::More;
use Test::Deep;
use DBI;
use DBD::MariaDB;
use lib '.', 't';
require 'lib.pl';
$|= 1;

use vars qw($test_dsn $test_user $test_password);

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

plan tests => 70*2;

for my $mariadb_server_prepare (0, 1) {
$dbh->{mariadb_server_prepare} = $mariadb_server_prepare;

ok($dbh->do(qq{DROP TABLE IF EXISTS t_dbd_40types}), "making slate clean");

ok($dbh->do(qq{CREATE TABLE t_dbd_40types (num INT)}), "creating table");
ok($dbh->do(qq{INSERT INTO t_dbd_40types VALUES (100)}), "loading data");

my ($val) = $dbh->selectrow_array("SELECT * FROM t_dbd_40types");
is($val, 100);

my $sv = svref_2object(\$val);
ok($sv->FLAGS & SVf_IOK, "scalar is integer");
ok(!($sv->FLAGS & (SVf_IVisUV|SVf_NOK|SVf_POK)), "scalar is not unsigned intger or double or string");

my $sth = $dbh->prepare("SELECT * FROM t_dbd_40types");
ok($sth->execute());
($val) = $sth->fetchrow_array();
is($val, 100);

$sv = svref_2object(\$val);
ok($sv->FLAGS & SVf_IOK, "scalar is integer");
ok(!($sv->FLAGS & (SVf_IVisUV|SVf_NOK|SVf_POK)), "scalar is not unsigned intger or double or string");

is_deeply($sth->{TYPE}, [ DBI::SQL_INTEGER ], "checking column type");
is_deeply($sth->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ], "checking mariadb column type");

ok($dbh->do(qq{DROP TABLE t_dbd_40types}), "cleaning up");

ok($dbh->do(qq{CREATE TABLE t_dbd_40types (num VARCHAR(10))}), "creating table");
ok($dbh->do(qq{INSERT INTO t_dbd_40types VALUES ('string')}), "loading data");

($val) = $dbh->selectrow_array("SELECT * FROM t_dbd_40types");
is($val, "string");

$sv = svref_2object(\$val);
ok($sv->FLAGS & SVf_POK, "scalar is string");
ok(!($sv->FLAGS & (SVf_IOK|SVf_NOK)), "scalar is not intger or double");

$sth = $dbh->prepare("SELECT * FROM t_dbd_40types");
ok($sth->execute());
($val) = $sth->fetchrow_array();
is($val, "string");

$sv = svref_2object(\$val);
ok($sv->FLAGS & SVf_POK, "scalar is string");
ok(!($sv->FLAGS & (SVf_IOK|SVf_NOK)), "scalar is not intger or double");

is_deeply($sth->{TYPE}, [ DBI::SQL_VARCHAR ], "checking column type");
cmp_deeply($sth->{mariadb_type}, [ any(DBD::MariaDB::TYPE_VARCHAR, DBD::MariaDB::TYPE_VAR_STRING) ], "checking mariadb column type");

ok($dbh->do(qq{DROP TABLE t_dbd_40types}), "cleaning up");

SKIP: {
skip "Clients < 5.0.3 do not support new decimal type from servers >= 5.0.3", 6 if $dbh->{mariadb_serverversion} >= 50003 and $dbh->{mariadb_clientversion} < 50003;

ok($dbh->do(qq{CREATE TABLE t_dbd_40types (d DECIMAL(5,2))}), "creating table");

$sth= $dbh->prepare("SELECT * FROM t_dbd_40types WHERE 1 = 0");
ok($sth->execute(), "getting table information");

is_deeply($sth->{TYPE}, [ DBI::SQL_DECIMAL ], "checking column type");
cmp_deeply($sth->{mariadb_type}, [ any(DBD::MariaDB::TYPE_DECIMAL, DBD::MariaDB::TYPE_NEWDECIMAL) ], "checking mariadb column type");

ok($dbh->do(qq{DROP TABLE t_dbd_40types}), "cleaning up");
}

#
# Bug #23936: bind_param() doesn't work with SQL_DOUBLE datatype
# Bug #24256: Another failure in bind_param() with SQL_DOUBLE datatype
#
ok($dbh->do(qq{CREATE TABLE t_dbd_40types (num DOUBLE)}), "creating table");

$sth= $dbh->prepare("INSERT INTO t_dbd_40types VALUES (?)");
ok($sth->bind_param(1, 2.1, DBI::SQL_DOUBLE), "binding parameter");
ok($sth->execute(), "inserting data");
ok($sth->bind_param(1, -1, DBI::SQL_DOUBLE), "binding parameter");
ok($sth->execute(), "inserting data");

my $ret = $dbh->selectall_arrayref("SELECT * FROM t_dbd_40types");
cmp_deeply($ret, [ [num(2.1, 0.00001)], [num(-1, 0.00001)] ]);

$sv = svref_2object(\$ret->[0]->[0]);
ok($sv->FLAGS & SVf_NOK, "scalar is double");
ok(!($sv->FLAGS & (SVf_IOK|SVf_POK)), "scalar is not integer or string");

$sv = svref_2object(\$ret->[1]->[0]);
ok($sv->FLAGS & SVf_NOK, "scalar is double");
ok(!($sv->FLAGS & (SVf_IOK|SVf_POK)), "scalar is not integer or string");

$sth = $dbh->prepare("SELECT * FROM t_dbd_40types");
ok($sth->execute());
$ret = $sth->fetchall_arrayref();
cmp_deeply($ret, [ [num(2.1, 0.00001)], [num(-1, 0.00001)] ]);

$sv = svref_2object(\$ret->[0]->[0]);
ok($sv->FLAGS & SVf_NOK, "scalar is double");
ok(!($sv->FLAGS & (SVf_IOK|SVf_POK)), "scalar is not integer or string");

$sv = svref_2object(\$ret->[1]->[0]);
ok($sv->FLAGS & SVf_NOK, "scalar is double");
ok(!($sv->FLAGS & (SVf_IOK|SVf_POK)), "scalar is not integer or string");

is_deeply($sth->{TYPE}, [ DBI::SQL_DOUBLE ], "checking column type");
is_deeply($sth->{mariadb_type}, [ DBD::MariaDB::TYPE_DOUBLE ], "checking mariadb column type");

ok($dbh->do(qq{DROP TABLE t_dbd_40types}), "cleaning up");

#
# [rt.cpan.org #19212] Mysql Unsigned Integer Fields
#
ok($dbh->do(qq{CREATE TABLE t_dbd_40types (num INT UNSIGNED)}), "creating table");
ok($dbh->do(qq{INSERT INTO t_dbd_40types VALUES (0),(4294967295)}), "loading data");

$ret = $dbh->selectall_arrayref("SELECT * FROM t_dbd_40types");
is_deeply($ret, [ [0],  [4294967295] ]);

$sv = svref_2object(\$ret->[0]->[0]);
ok($sv->FLAGS & (SVf_IOK|SVf_IVisUV), "scalar is unsigned integer");
ok(!($sv->FLAGS & (SVf_NOK|SVf_POK)), "scalar is not double or string");

$sv = svref_2object(\$ret->[1]->[0]);
ok($sv->FLAGS & (SVf_IOK|SVf_IVisUV), "scalar is unsigned integer");
ok(!($sv->FLAGS & (SVf_NOK|SVf_POK)), "scalar is not double or string");

$sth = $dbh->prepare("SELECT * FROM t_dbd_40types");
ok($sth->execute());
$ret = $sth->fetchall_arrayref();
is_deeply($ret, [ [0], [4294967295] ]);

$sv = svref_2object(\$ret->[0]->[0]);
ok($sv->FLAGS & (SVf_IOK|SVf_IVisUV), "scalar is unsigned integer");
ok(!($sv->FLAGS & (SVf_NOK|SVf_POK)), "scalar is not double or string");

$sv = svref_2object(\$ret->[1]->[0]);
ok($sv->FLAGS & (SVf_IOK|SVf_IVisUV), "scalar is unsigned integer");
ok(!($sv->FLAGS & (SVf_NOK|SVf_POK)), "scalar is not double or string");

is_deeply($sth->{TYPE}, [ DBI::SQL_INTEGER ], "checking column type");
is_deeply($sth->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ], "checking mariadb column type");

ok($dbh->do(qq{DROP TABLE t_dbd_40types}), "cleaning up");

# https://github.com/gooddata/DBD-MariaDB/issues/109: Check DBI::SQL_BIGINT type
ok($dbh->do(qq{CREATE TABLE t_dbd_40types (num BIGINT)}), "creating table for bigint");
$sth = $dbh->prepare("SELECT * FROM t_dbd_40types");
ok($sth->execute());
is_deeply($sth->{TYPE}, [ DBI::SQL_BIGINT ], "checking column type of bigint");
is_deeply($sth->{mariadb_type}, [ DBD::MariaDB::TYPE_LONGLONG ], "checking mariadb column type of bigint");
ok($dbh->do(qq{DROP TABLE t_dbd_40types}), "cleaning up");

}

$dbh->disconnect();

