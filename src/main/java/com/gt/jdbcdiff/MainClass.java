package com.gt.jdbcdiff;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MainClass {

	public static void main(String[] args) throws SQLException, ClassNotFoundException, FileNotFoundException, IOException {

		if (args == null || args.length == 0) {
			args = new String[] { 
					"-d1", "org.postgresql.Driver", 
					"-d2", "org.postgresql.Driver", 
					"-j1", "jdbc:postgresql://192.168.80.5/gcem_pos", 
					"-j2", "jdbc:postgresql://192.168.80.5/gcem_pos_viejo", 
					"-u1", "carnave", 
					"-u2", "carnave", 
					"-p1", "Quilmes", 
					"-p2", "Quilmes"};
		}

		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(MainClass.buildOptions(), args);
		} catch (ParseException ex) {
			Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, "Error al leer las opciones", ex);
			return;
		}

		if (commandLine.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Ayuda de jdbcbackup", MainClass.buildOptions());
		} else {
			Class.forName(commandLine.getOptionValue("d1"));
			Class.forName(commandLine.getOptionValue("d2"));

			Connection c1 = DriverManager.getConnection(commandLine.getOptionValue("j1"), commandLine.getOptionValue("u1"),
					commandLine.getOptionValue("p1"));
			Connection c2 = DriverManager.getConnection(commandLine.getOptionValue("j2"), commandLine.getOptionValue("u2"),
					commandLine.getOptionValue("p2"));

			DbDiff dbDiff = new DbDiff(c1, c2, System.out);
			dbDiff.comparar();
			
			dbDiff = new DbDiff(c2, c1, System.out);
			dbDiff.comparar();

			c1.close();
			c2.close();
		}
	}

	public static Options buildOptions() {

		Options options = new Options();

		options.addOption(Option.builder("h").longOpt("help").argName("comando").build());

		options.addOption(Option.builder("d1").longOpt("driverclass1").hasArg().desc("Clase java del driver jdbc conexión 1").build());
		options.addOption(Option.builder("j1").longOpt("jdbc1").hasArg().desc("jdbc connection string conexión 1").build());
		options.addOption(Option.builder("u1").longOpt("uid1").hasArg().desc("Nombre de usuario conexión 1").build());
		options.addOption(Option.builder("p1").longOpt("pass1").hasArg().desc("contraseña conexión 1").build());
		options.addOption(Option.builder("f1").longOpt("file1").hasArg().desc("base de datos conexión 1").build());
		options.addOption(Option.builder("d2").longOpt("driverclass2").hasArg().desc("Clase java del driver jdbc conexión 2").build());
		options.addOption(Option.builder("j2").longOpt("jdbc2").hasArg().desc("jdbc connection string conexión 2").build());
		options.addOption(Option.builder("u2").longOpt("uid2").hasArg().desc("Nombre de usuario conexión 2").build());
		options.addOption(Option.builder("p2").longOpt("pass2").hasArg().desc("contraseña conexión 2").build());
		options.addOption(Option.builder("f2").longOpt("file2").hasArg().desc("base de datos conexión 2").build());

		return options;
	}

	public static String getJarPath() {
		CodeSource codeSource = MainClass.class.getProtectionDomain().getCodeSource();

		String jarDir = "";
		try {
			File jarFile = new File(codeSource.getLocation().toURI().getPath());
			jarDir = jarFile.getParentFile().getPath();
		} catch (URISyntaxException ex) {
			java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.SEVERE, "Error buscando jar path", ex);
		}
		return jarDir;
	}

}
