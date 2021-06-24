package com.gt.jdbcdiff;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DbDiff {

	Connection c1;
	Connection c2;

	OutputStream outstream;

	public DbDiff(Connection c1, Connection c2, OutputStream outStream) {
		super();
		this.c1 = c1;
		this.c2 = c2;
		this.outstream = outStream;
	}

	public void comparar() {
		try {
			write("\n\n");
			write("-- INICIO comparacion " + c1.getMetaData().getURL() + " con " + c2.getMetaData().getURL());
			write("\n\n");

			diffTablas(c1, c2);

			write("-- Fin comparacion " + c1.getMetaData().getURL() + " con " + c2.getMetaData().getURL());
			write("\n\n\n\n");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void diffTablas(Connection izquierda, Connection derecha) throws SQLException, IOException {

		ResultSet rs1 = this.c1.getMetaData().getTables(this.c1.getCatalog(), null, "%", new String[] { "TABLE" });
		ResultSet rs2 = this.c2.getMetaData().getTables(this.c2.getCatalog(), null, "%", new String[] { "TABLE" });

		String tabla1;
		while (rs1.next()) {
			tabla1 = rs1.getString("TABLE_SCHEM") + "." + rs1.getString("TABLE_NAME");
			rs2.beforeFirst();
			boolean esta = false;
			while (rs2.next()) {
				if (tabla1.equalsIgnoreCase(rs2.getString("TABLE_SCHEM") + "." + rs2.getString("TABLE_NAME"))) {
					esta = true;
					break;
				}
			}
			if (!esta) {
				getCreateScript(izquierda, rs1.getString("TABLE_SCHEM"), rs1.getString("TABLE_NAME"));
			} else {
				// comparo los campos
				compararCampos(izquierda, derecha, rs1.getString("TABLE_SCHEM"), rs1.getString("TABLE_NAME"));
			}
		}
	}

	public Connection getC1() {
		return c1;
	}

	public void setC1(Connection c1) {
		this.c1 = c1;
	}

	public Connection getC2() {
		return c2;
	}

	public void setC2(Connection c2) {
		this.c2 = c2;
	}

	private void write(String content) {
		try {
			this.outstream.write(content.getBytes());
		} catch (IOException ex) {
			Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void compararCampos(Connection izquierda, Connection derecha, String schemaName, String tableName) throws SQLException {

		DatabaseMetaData metaData1 = izquierda.getMetaData();

		ResultSet rs1 = metaData1.getColumns(izquierda.getCatalog(), schemaName, tableName, "%");

		DatabaseMetaData metaData2 = derecha.getMetaData();

		ResultSet rs2 = metaData2.getColumns(derecha.getCatalog(), schemaName, tableName, "%");

		while (rs1.next()) {
			boolean esta = false;
			while (rs2.next()) {
				if (rs1.getString("COLUMN_NAME").equalsIgnoreCase(rs2.getString("COLUMN_NAME"))) {
					esta = true;
					break;
				}
			}
			if (!esta) {
				write("-- falta campo " + rs1.getString("COLUMN_NAME") + "\n");
				write("ALTER TABLE " + schemaName + "." + tableName + " ADD COLUMN ");
				fieldDef(izquierda, rs1, false);
				write(";\n");
			} else {
				// me fijo que sean del mismo tipo
				if (!getTipoSQL(rs1).equalsIgnoreCase(getTipoSQL(rs2))) {
					write("-- cambio de tipo campo " + rs1.getString("COLUMN_NAME") + "\n");
					write("ALTER TABLE " + schemaName + "." + tableName);
					
					String originalURL = derecha.getMetaData().getURL();
					Driver drv = DriverManager.getDriver(originalURL);
					String driverClass = drv.getClass().getName();
					
					
					switch (driverClass) {
					case "com.microsoft.sqlserver.jdbc.SQLServerDriver":
						write(" ALTER COLUMN " + rs1.getString("COLUMN_NAME"));
						write(" " + getTipoSQL(rs1));
						break;
					case "org.postgresql.Driver":
						write(" ALTER COLUMN " + rs1.getString("COLUMN_NAME"));
						write(" TYPE " + getTipoSQL(rs1));
						break;
					default:
						write(" MODIFY COLUMN " + rs1.getString("COLUMN_NAME"));
						write(" " + getTipoSQL(rs1));
					}
					write(";\n");

				}
			}
		}
	}

	private void getCreateScript(Connection connection, String schemaName, String tableName) {
		Logger.getLogger(getClass().getName()).log(Level.FINE,
				"Generando script create de tabla " + schemaName + "." + tableName);

		write("CREATE TABLE ");
		write(schemaName);
		write(".");
		write(tableName);
		write(" (\n");
		try {
			DatabaseMetaData metaData = connection.getMetaData();

			ResultSet rs = metaData.getColumns(connection.getCatalog(), schemaName, tableName, "%");

			boolean first = true;

			boolean tieneIsAutoinc = false;
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				if (rs.getMetaData().getColumnName(i).equalsIgnoreCase("IS_AUTOINCREMENT")) {
					tieneIsAutoinc = true;
					break;
				}
			}
			while (rs.next()) {
				if (first) {
					first = false;
				} else {
					write(",\n");
				}
				fieldDef(connection, rs, tieneIsAutoinc);
			}
			rs = metaData.getPrimaryKeys(connection.getCatalog(), schemaName, tableName);

			first = true;
			while (rs.next()) {
				if (first) {
					write(",\n");
					write("PRIMARY KEY (");
					first = false;
				} else {
					write(", ");
				}
				write(rs.getString("COLUMN_NAME"));
			}
			if (!first) {
				write(")");
			}
		} catch (SQLException ex) {
			Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
			System.exit(1);
		}
		write(");\n\n");
	}

	private void fieldDef(Connection connection, ResultSet rs, boolean tieneIsAutoinc) throws SQLException {
		write(rs.getString("COLUMN_NAME"));
		write(" ");
		if ((tieneIsAutoinc) && (rs.getString("IS_AUTOINCREMENT") != null)
				&& (rs.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes"))) {
			switch (connection.getClass().getName()) {
			case "com.microsoft.sqlserver.jdbc.SQLServerDriver":
				write("IDENTITY(1,1)");
				break;
			case "org.postgresql.Driver":
				write("SERIAL");
				break;
			default:
				write(rs.getString("TYPE_NAME"));
				write("(");
				write(rs.getInt("COLUMN_SIZE") + "");
			}
		} else {
			String tipoSQL = getTipoSQL(rs);

			write(tipoSQL);

			if (tipoSQL.equals("VARCHAR")) {
				write("(");
				write(rs.getInt("COLUMN_SIZE") + "");
				write(")");
			}
		}
		if (rs.getInt("NULLABLE") == 0) {
			write(" NOT NULL");
		}
		if ((rs.getString("COLUMN_DEF") != null)
				&& (!rs.getString("COLUMN_DEF").isEmpty())) {
			write(" DEFAULT '");
			write(rs.getString("COLUMN_DEF"));
			write("'");
		}
	}

	private String getTipoSQL(ResultSet rs) throws SQLException {
		String tipoSQL = rs.getString("TYPE_NAME");
		if (tipoSQL.equals("CLOB")) {
			tipoSQL = "TEXT";
		} else if (tipoSQL.equals("VARBINARY")) {
			tipoSQL = "BYTEA";
		} else if (tipoSQL.equals("DOUBLE")) {
			tipoSQL = "DOUBLE PRECISION";
		}

		return tipoSQL;
	}
}
