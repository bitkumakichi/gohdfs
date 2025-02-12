package main

import (
	"fmt"
	"github.com/pborman/getopt"
	"github.com/bitkumakichi/gohdfs"
	"github.com/bitkumakichi/gohdfs/hadoopconf"
	krb "gopkg.in/jcmturner/gokrb5.v5/client"
	"gopkg.in/jcmturner/gokrb5.v5/config"
	"gopkg.in/jcmturner/gokrb5.v5/keytab"
	"net"
	"os"
	"os/user"
	"strings"
	"time"
)

// TODO: cp, tree, test, trash

var (
	version string
	usage   = fmt.Sprintf(`Usage: %s COMMAND
The flags available are a subset of the POSIX ones, but should behave similarly.

Valid commands:
  ls [-lah] [FILE]...
  rm [-rf] FILE...
  mv [-nT] SOURCE... DEST
  cp [-r] SOURCE DEST
  mkdir [-p] FILE...
  touch [-amc] FILE...
  chmod [-R] OCTAL-MODE FILE...
  chown [-R] OWNER[:GROUP] FILE...
  cat SOURCE...
  head [-n LINES | -c BYTES] SOURCE...
  tail [-n LINES | -c BYTES] SOURCE...
  du [-sh] FILE...
  checksum FILE...
  get SOURCE [DEST]
  getmerge SOURCE DEST
  put SOURCE DEST
  df [-h]
`, os.Args[0])

	lsOpts = getopt.New()
	lsl    = lsOpts.Bool('l')
	lsa    = lsOpts.Bool('a')
	lsh    = lsOpts.Bool('h')

	rmOpts = getopt.New()
	rmr    = rmOpts.Bool('r')
	rmf    = rmOpts.Bool('f')

	mvOpts = getopt.New()
	mvn    = mvOpts.Bool('n')
	mvT    = mvOpts.Bool('T')

	cpOpts = getopt.New()
	cpr    = cpOpts.Bool('r')

	mkdirOpts = getopt.New()
	mkdirp    = mkdirOpts.Bool('p')

	touchOpts = getopt.New()
	touchc    = touchOpts.Bool('c')

	chmodOpts = getopt.New()
	chmodR    = chmodOpts.Bool('R')

	chownOpts = getopt.New()
	chownR    = chownOpts.Bool('R')

	headTailOpts = getopt.New()
	headtailn    = headTailOpts.Int64('n', -1)
	headtailc    = headTailOpts.Int64('c', -1)

	duOpts = getopt.New()
	dus    = duOpts.Bool('s')
	duh    = duOpts.Bool('h')

	getmergeOpts = getopt.New()
	getmergen    = getmergeOpts.Bool('n')

	dfOpts = getopt.New()
	dfh    = dfOpts.Bool('h')

	cachedClients map[string]*hdfs.Client = make(map[string]*hdfs.Client)
	status                                = 0
)

func init() {
	lsOpts.SetUsage(printHelp)
	rmOpts.SetUsage(printHelp)
	mvOpts.SetUsage(printHelp)
	cpOpts.SetUsage(printHelp)
	touchOpts.SetUsage(printHelp)
	chmodOpts.SetUsage(printHelp)
	chownOpts.SetUsage(printHelp)
	headTailOpts.SetUsage(printHelp)
	duOpts.SetUsage(printHelp)
	getmergeOpts.SetUsage(printHelp)
	dfOpts.SetUsage(printHelp)
}

func main() {
	if len(os.Args) < 2 {
		printHelp()
	}

	command := os.Args[1]
	argv := os.Args[1:]
	switch command {
	case "-v", "--version":
		fatal("gohdfs version", version)
	case "ls":
		lsOpts.Parse(argv)
		ls(lsOpts.Args(), *lsl, *lsa, *lsh)
	case "rm":
		rmOpts.Parse(argv)
		rm(rmOpts.Args(), *rmr, *rmf)
	case "mv":
		mvOpts.Parse(argv)
		mv(mvOpts.Args(), !*mvn, *mvT)
	case "cp":
		cpOpts.Parse(argv)
		cp(cpOpts.Args(), *cpr)
	case "mkdir":
		mkdirOpts.Parse(argv)
		mkdir(mkdirOpts.Args(), *mkdirp)
	case "touch":
		touchOpts.Parse(argv)
		touch(touchOpts.Args(), *touchc)
	case "chown":
		chownOpts.Parse(argv)
		chown(chownOpts.Args(), *chownR)
	case "chmod":
		chmodOpts.Parse(argv)
		chmod(chmodOpts.Args(), *chmodR)
	case "cat":
		cat(argv[1:])
	case "head", "tail":
		headTailOpts.Parse(argv)
		printSection(headTailOpts.Args(), *headtailn, *headtailc, (command == "tail"))
	case "du":
		duOpts.Parse(argv)
		du(duOpts.Args(), *dus, *duh)
	case "checksum":
		checksum(argv[1:])
	case "get":
		get(argv[1:])
	case "getmerge":
		getmergeOpts.Parse(argv)
		getmerge(getmergeOpts.Args(), *getmergen)
	case "put":
		put(argv[1:])
	case "df":
		dfOpts.Parse(argv)
		df(*dfh)
	// it's a seeeeecret command
	case "complete":
		complete(argv)
	case "help", "-h", "-help", "--help":
		printHelp()
	default:
		fatalWithUsage("Unknown command:", command)
	}

	os.Exit(status)
}

func printHelp() {
	fmt.Fprintln(os.Stderr, usage)
	os.Exit(0)
}

func fatal(msg ...interface{}) {
	fmt.Fprintln(os.Stderr, msg...)
	os.Exit(1)
}

func fatalWithUsage(msg ...interface{}) {
	msg = append(msg, "\n"+usage)
	fatal(msg...)
}

func findRealm(krbConf *config.Config) string {
	realm := krbConf.LibDefaults.DefaultRealm
	if realm != "" {
		return realm
	}
	return "HADOOP.COM"
}

func getClientNormal(userName string) (hdfs.ClientOptions, error) {
	var options hdfs.ClientOptions
	namenode := os.Getenv("HADOOP_NAMENODE")
	if namenode != "" {
		options.Addresses = strings.Split(namenode, "_")
	}
	options.User = userName
	return options, nil
}

func getClientKerberos(userName string, hadoopConfDir string) (hdfs.ClientOptions, error) {
	conf, err := hadoopconf.Load(hadoopConfDir)
	var options hdfs.ClientOptions
	if err != nil {
		return options, err
	}
	options = hdfs.ClientOptionsFromConf(conf)
	ktab, err := keytab.Load(hadoopConfDir + "/user.keytab")
	if err != nil {
		return options, err
	}
	krbConf, err := config.Load(hadoopConfDir + "/krb5.conf")
	if err != nil {
		return options, err
	}
	realm := findRealm(krbConf)
	kerberosClient := krb.NewClientWithKeytab(userName, realm, ktab)
	options.KerberosClient = &kerberosClient
	options.KerberosClient.Config = krbConf
	err = options.KerberosClient.Login()
	return options, err
}

func getClient(namenode string) (*hdfs.Client, error) {
	if cachedClients[namenode] != nil {
		return cachedClients[namenode], nil
	}
	userName := os.Getenv("HADOOP_USER_NAME")
	if userName == "" {
		u, err := user.Current()
		if err != nil {
			return nil, err
		}
		userName = u.Username
	}
	options, err := getClientNormal(userName)
	if err != nil {
		return nil, err
	}
	// For only check of kerberos is hdfs_conf dir
	hadoopConfDir := "hdfs_conf"
	if _, err := os.Stat(hadoopConfDir); !os.IsNotExist(err) {
		i := 1
		for i <= 1000 {
			options, err = getClientKerberos(userName, hadoopConfDir)
			if err == nil {
				break
			}
			os.Stderr.WriteString(fmt.Sprintf("count: %d %s\n", i, err.Error()))
			time.Sleep(time.Duration(i) * time.Second)
			i++
		}
		if err != nil {
			return nil, err
		}
	}

	dialFunc := (&net.Dialer{
		Timeout:   300000 * time.Second,
		KeepAlive: 300000 * time.Second,
		Deadline:  time.Now().Add(300000 * time.Second),
		DualStack: true,
	}).DialContext
	options.NamenodeDialFunc = dialFunc
	options.DatanodeDialFunc = dialFunc
	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("Couldn't connect to namenode: %s", err)
	}
	cachedClients[namenode] = c
	return c, nil
}
