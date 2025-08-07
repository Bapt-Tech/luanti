#include "database-sqlite.h"
#include "debug.h"
#include <filesystem>

PlayerPositionDatabaseSQLite::PlayerPositionDatabaseSQLite(const std::string &savedir) {
    db_path = savedir + "/player_positions.sqlite";
    db = nullptr;
}

PlayerPositionDatabaseSQLite::~PlayerPositionDatabaseSQLite() {
    if(db) {
        sqlite3_close(db);
    }
}

bool PlayerPositionDatabaseSQLite::initDatabase() {
    if(sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
        errorstream << "Failed to open SQLite database: " << sqlite3_errmsg(db) << std::endl;
        return false;
    }

    const char *sql = "CREATE TABLE IF NOT EXISTS player_positions ("
                     "name TEXT PRIMARY KEY,"
                     "pitch REAL,"
                     "yaw REAL," 
                     "posX REAL,"
                     "posY REAL,"
                     "posZ REAL)";

    char *err_msg = nullptr;
    if(sqlite3_exec(db, sql, nullptr, nullptr, &err_msg) != SQLITE_OK) {
        errorstream << "SQL error: " << err_msg << std::endl;
        sqlite3_free(err_msg);
        return false;
    }

    return true;
}

bool PlayerPositionDatabaseSQLite::savePlayerPosition(const std::string &name, 
    float pitch, float yaw, float posX, float posY, float posZ) {
    
    const char *sql = "INSERT OR REPLACE INTO player_positions "
                     "(name, pitch, yaw, posX, posY, posZ) VALUES (?,?,?,?,?,?)";
                     
    sqlite3_stmt *stmt;
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }

    sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 2, pitch);
    sqlite3_bind_double(stmt, 3, yaw);
    sqlite3_bind_double(stmt, 4, posX);
    sqlite3_bind_double(stmt, 5, posY);
    sqlite3_bind_double(stmt, 6, posZ);

    bool success = sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    
    return success;
}

bool PlayerPositionDatabaseSQLite::loadPlayerPosition(const std::string &name,
    float &pitch, float &yaw, float &posX, float &posY, float &posZ) {
    
    const char *sql = "SELECT pitch, yaw, posX, posY, posZ FROM player_positions WHERE name = ?";
    
    sqlite3_stmt *stmt;
    if(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }

    sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

    if(sqlite3_step(stmt) == SQLITE_ROW) {
        pitch = sqlite3_column_double(stmt, 0);
        yaw = sqlite3_column_double(stmt, 1);
        posX = sqlite3_column_double(stmt, 2);
        posY = sqlite3_column_double(stmt, 3);
        posZ = sqlite3_column_double(stmt, 4);
        sqlite3_finalize(stmt);
        return true;
    }

    sqlite3_finalize(stmt);
    return false;
}