package src.main.java;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.log4j.Logger;

/*
USE Analyst
GO

DROP TABLE dbo.Taxonomy
GO

CREATE TABLE dbo.Taxonomy
   (ID int PRIMARY KEY NOT NULL,
    Code varchar(64) NOT NULL,
	Name varchar(128) NOT NULL,
	CodeDefinition varchar(MAX),
    CreatedDate date,
	LastModifiedDate date,
	ImportedDate date NOT NULL default CURRENT_TIMESTAMP)
GO

CREATE INDEX Taxonomy_Code_Index ON dbo.Taxonomy (Code)
GO
*/
public class DbTaxonomy {
    private static final Logger log = Logger.getLogger(DbTaxonomy.class.getName());

    public long id = 0;
    public String code;
    public String name;
    public String definition;
    public Date created;
    public Date lastModified;

    public static DbTaxonomy findByCode(Connection conn, String code) {
        DbTaxonomy result = null;
        try {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT ID, Code, Name, CodeDefinition, CreatedDate, ");
            sb.append("LastModifiedDate, ImportedDate ");
            sb.append("FROM dbo.Taxonomy ");
            sb.append("WHERE Code = '" + code + "'");

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sb.toString());
            if (rs.next()) {
                result = new DbTaxonomy();
                result.id = rs.getLong("ID");
                result.code = rs.getString("Code");
                result.name = rs.getString("Name");
                result.definition = rs.getString("CodeDefinition");
                result.created = rs.getDate("CreatedDate");
                result.lastModified = rs.getDate("LastModifiedDate");
            }

            rs.close();
            stmt.close();
        }
        catch (SQLException sqle) {
             log.error("SQLException in DbTaxonomy.findByCode(): " + sqle);
        }
         catch (Exception e) {
             log.error("Exception in DbTaxonomy.findByCode(): " + e);
        }
        return result;
    }

    public void insert(Connection conn) {
        try {
            StringBuffer sb = new StringBuffer();
            sb.append("INSERT INTO dbo.Taxonomy (Code, Name, CodeDefinition, ");
            sb.append("CreatedDate, LastModifiedDate) ");
            sb.append("VALUES (?, ?, ?, ?, ?)");

            PreparedStatement ps = conn.prepareStatement(sb.toString());
            ps.setString(1, SqlString.encode(this.code));
            ps.setString(2, SqlString.encode(this.name));
            ps.setString(3, SqlString.encode(this.definition));

            java.sql.Date sqlDate = null;
            if (this.created != null) {
                sqlDate = new java.sql.Date(this.created.getTime());
                ps.setDate(4, sqlDate);
            }
            else {
                ps.setDate(4, null);
            }
            if (this.lastModified != null) {
                sqlDate = new java.sql.Date(this.lastModified.getTime());
                ps.setDate(5, sqlDate);
            }
            else {
                ps.setDate(5, null);
            }

            int out = ps.executeUpdate();
            if (out == 0) {
                log.info("Failed to insert taxonomy record!");
            }
        }
        catch (SQLException sqle) {
             log.error("SQLException in DbTaxonomy.insert(): " + sqle);
        }
         catch (Exception e) {
             log.error("Exception in DbTaxonomy.insert(): " + e);
        }
    }

    public void update(Connection conn) {
        try {
            StringBuffer sb = new StringBuffer();
            sb.append("UPDATE dbo.Taxonomy SET Code = ?, Name = ?, CodeDefinition = ?, ");
            sb.append("CreatedDate = ?, LastModifiedDate = ?, importedDate = ? ");
            sb.append("WHERE ID = ?");

            PreparedStatement ps = conn.prepareStatement(sb.toString());
            ps.setString(1, SqlString.encode(this.code));
            ps.setString(2, SqlString.encode(this.name));
            ps.setString(3, SqlString.encode(this.definition));

            java.sql.Date sqlDate = null;
            if (this.created != null) {
                sqlDate = new java.sql.Date(this.created.getTime());
                ps.setDate(4, sqlDate);
            }
            else {
                ps.setDate(4, null);
            }
            if (this.lastModified != null) {
                sqlDate = new java.sql.Date(this.lastModified.getTime());
                ps.setDate(5, sqlDate);
            }
            else {
                ps.setDate(5, null);
            }

            Date now = new Date();
            sqlDate = new java.sql.Date(now.getTime());
            ps.setDate(6, sqlDate);
            ps.setLong(7, this.id);

            int out = ps.executeUpdate();
            if (out == 0) {
                log.info("Failed to update taxonomy record!");
            }
        }
        catch (SQLException sqle) {
             log.error("SQLException in DbTaxonomy.update(): " + sqle);
        }
         catch (Exception e) {
             log.error("Exception in DbTaxonomy.update(): " + e);
        }
    }
}
