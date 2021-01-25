import sys, config, optparse, logging, os, shutil, subprocess, pymysql 
from logging.handlers import RotatingFileHandler
from datetime import datetime

filename, file_extension = os.path.splitext(os.path.basename(__file__))
formatter = logging.Formatter('%(asctime)s - %(levelname)10s - %(module)15s:%(funcName)30s:%(lineno)5s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)
logging.getLogger("requests").setLevel(logging.WARNING)
logger.setLevel(config.log_level)
fileHandler = RotatingFileHandler(config.log_folder + '/' + filename + '.log', maxBytes=1024 * 1024 * 1, backupCount=1)
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

def cleanup_backups():
	paths = config.path.split(";")
	for path in paths:
		bin_path = config.recycle_bin_path + "/" + path
		try:
			curr_path = "%s/%s" % (config.backup_path, path)
			for subdir, dirs, files in os.walk("%s/%s" % (config.backup_path, path)):
				if len(files) > config.minium_files:
					for file in files:
						try:
							(filename, extension) = os.path.splitext(file)
							if abs((datetime.strptime(filename, "%Y%m%d %H%M%S") - datetime.today()).days) > config.backup_days:
								try:
									os.makedirs(bin_path, exist_ok=True)
									shutil.move(curr_path + "/" + file, bin_path + "/" + file)
									logger.info("Moved %s to Recycle Bin" % (curr_path + "/" + file))
								except Exception as e:
									logger.error("Error - Moving %s - %s" % (curr_path + "/" + file, e))
						except Exception as e:
							logger.error("Error - %s - %s" % (curr_path + "/" + file, e))
				else:
					logger.info("Less Than Minimum Number of Files, Not Removing Any")
		except Exception as e:
			logger.error("Error - Cleaning Up Backups - %s" % e)

def sql_backup():
	x = 0
	try:
		conn = pymysql.connect(host=config.mysql_host, user=config.mysql_user, passwd=config.mysql_passwd, autocommit=True, use_unicode=True, charset="utf8")
		cur = conn.cursor()
		cur.execute("show databases;")
		databases = list(cur)
		cur.close
		
		current_datetime = datetime.now().strftime("%Y%m%d %H%M%S")
		folder_name  = "%s/SQL Database/%s" % (config.backup_path, datetime.now().strftime("%Y-%m-%d"))
		os.makedirs(folder_name, exist_ok=True)
		for x, db in enumerate(databases):
			try:
				db = db[0]
				logger.info("(%s/%s) Backing Up SQL Database: %s" % (x+1, len(databases), db))
				filename = "%s/%s - %s.sql" % (folder_name, current_datetime, db)
				command = 'mysqldump -h %s -u %s -p%s --databases %s' % (config.mysql_host, config.mysql_user, config.mysql_passwd, db)
				with open(filename,'w') as output:
					c = subprocess.Popen(command, stdout=output, shell=True)
					c.wait()
			except Exception as e:
				logger.error("Error Backing Up Database: %s - %s" % (db, e))
				
		shutil.make_archive(current_datetime, 'zip', folder_name)
		shutil.move('%s.zip' % current_datetime, "%s/SQL Database" % config.backup_path)
		shutil.rmtree(folder_name)
	except Exception as e:
		logger.error("Error Backing Up SQL Databases - %s" % e)

if __name__ == "__main__":
	error = False
	parser = optparse.OptionParser()
	parser.add_option('-c', '--clean-up', action="store_const", const=True, dest="cleanup_backups")
	parser.add_option('-s', '--sql-backup', action="store_const", const=True, dest="sql_backup")
