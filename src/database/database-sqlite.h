#pragma once
#include <string>
#include <sqlite3.h>

class PlayerPositionDatabaseSQLite {
public:
    PlayerPositionDatabaseSQLite(const std::string &savedir);
    ~PlayerPositionDatabaseSQLite();
    
    bool initDatabase();
    bool savePlayerPosition(const std::string &name, float pitch, float yaw, 
                          float posX, float posY, float posZ);
    bool loadPlayerPosition(const std::string &name, float &pitch, float &yaw,
                          float &posX, float &posY, float &posZ);

private:
    sqlite3 *db;
    std::string db_path;
};