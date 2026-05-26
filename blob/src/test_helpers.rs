//! Helpers for generating realistic test data (game databases, character trees, etc.)

use crate::arc_value::ArcValue;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

/// Load a JSON file from disk and convert it to an ArcValue.
pub fn load_json_as_arcvalue(path: &std::path::Path) -> ArcValue {
    let data = std::fs::read_to_string(path).expect("failed to read seed file");
    let value: serde_json::Value = serde_json::from_str(&data).expect("failed to parse JSON");
    ArcValue::from_value(value)
}

/// Wrap seed data as N games under a root: `{ "games": { "game_0": seed, "game_1": seed, ... } }`.
///
/// Since ArcValue uses `Arc`, cloning the seed data N times is cheap — all copies share the
/// underlying data. The blob writer will serialize each copy independently.
pub fn replicate_as_games(seed: &ArcValue, num_games: usize) -> ArcValue {
    let mut games = HashMap::new();
    for i in 0..num_games {
        games.insert(format!("game_{}", i), seed.clone());
    }
    let mut root = HashMap::new();
    root.insert("games".to_string(), ArcValue::Object(Arc::new(games)));
    ArcValue::Object(Arc::new(root))
}

/// Generate an example game database tree with the given number of games.
/// Each game has characters, pages, chat messages, and config.
///
/// Structure:
/// ```text
/// {
///   "games": {
///     "game_0": {
///       "characters": { "char_0": {hp, name, x, y, ...}, ... },
///       "pages": { "page_0": {name, grid, ...}, ... },
///       "chat": { "msg_0": {sender, text, timestamp}, ... },
///       "config": { ... }
///     },
///     ...
///   }
/// }
/// ```
pub fn generate_game_database(
    num_games: usize,
    chars_per_game: usize,
    pages_per_game: usize,
    messages_per_game: usize,
) -> ArcValue {
    let mut games = HashMap::new();

    for g in 0..num_games {
        let game_id = format!("game_{}", g);
        let mut game = HashMap::new();

        // Characters
        let mut characters = HashMap::new();
        for c in 0..chars_per_game {
            let char_id = format!("char_{}", c);
            let character = ArcValue::from_value(json!({
                "name": format!("Character {} in game {}", c, g),
                "hp": 100 + c as i64,
                "max_hp": 100 + c as i64,
                "x": c as f64 * 70.0,
                "y": g as f64 * 70.0,
                "width": 70,
                "height": 70,
                "layer": "objects",
                "represents": format!("player_{}", c % 6),
                "bar1_value": 100 + c as i64,
                "bar1_max": 100 + c as i64,
                "bar2_value": 50,
                "bar2_max": 100,
                "bar3_value": 10,
                "bar3_max": 20,
                "aura1_radius": "",
                "aura1_color": "#ffff00",
                "statusmarkers": "",
                "showname": true,
                "showplayers_name": true,
                "showplayers_bar1": true,
                "controlledby": format!("player_{}", c % 6)
            }));
            characters.insert(char_id, character);
        }
        game.insert(
            "characters".to_string(),
            ArcValue::Object(Arc::new(characters)),
        );

        // Pages
        let mut pages = HashMap::new();
        for p in 0..pages_per_game {
            let page_id = format!("page_{}", p);
            let page = ArcValue::from_value(json!({
                "name": format!("Page {}", p),
                "width": 25,
                "height": 25,
                "grid_type": "square",
                "grid_size": 70,
                "grid_color": "#C0C0C0",
                "grid_opacity": 0.5,
                "background_color": "#FFFFFF",
                "fog_opacity": 0.35,
                "showgrid": true,
                "showdarkness": false,
                "showlighting": false
            }));
            pages.insert(page_id, page);
        }
        game.insert("pages".to_string(), ArcValue::Object(Arc::new(pages)));

        // Chat messages
        let mut chat = HashMap::new();
        for m in 0..messages_per_game {
            let msg_id = format!("msg_{}", m);
            let msg = ArcValue::from_value(json!({
                "who": format!("Player {}", m % 6),
                "type": "general",
                "content": format!("This is message {} in game {}. It contains some text to make it realistic.", m, g),
                "playerid": format!("player_{}", m % 6),
                "timestamp": 1700000000 + m as i64
            }));
            chat.insert(msg_id, msg);
        }
        game.insert("chat".to_string(), ArcValue::Object(Arc::new(chat)));

        // Config
        game.insert(
            "config".to_string(),
            ArcValue::from_value(json!({
                "name": format!("Game {}", g),
                "created_at": 1700000000 + g as i64,
                "player_count": 6,
                "gm": "player_0",
                "turn_order": [],
                "initiative_page": false,
                "settings": {
                    "bar1_name": "HP",
                    "bar2_name": "AC",
                    "bar3_name": "Speed",
                    "advanced_shortcuts": false,
                    "compendium_override": null
                }
            })),
        );

        games.insert(game_id, ArcValue::Object(Arc::new(game)));
    }

    let mut root = HashMap::new();
    root.insert("games".to_string(), ArcValue::Object(Arc::new(games)));
    ArcValue::Object(Arc::new(root))
}

/// Generate all paths to leaf values within a tree, up to max_depth.
pub fn collect_leaf_paths(
    value: &ArcValue,
    prefix: &[String],
    max_depth: usize,
) -> Vec<Vec<String>> {
    if prefix.len() >= max_depth {
        return vec![prefix.to_vec()];
    }

    match value {
        ArcValue::Object(map) => {
            let mut paths = Vec::new();
            for (key, child) in map.iter() {
                let mut new_prefix = prefix.to_vec();
                new_prefix.push(key.clone());
                if child.is_primitive() {
                    paths.push(new_prefix);
                } else {
                    paths.extend(collect_leaf_paths(child, &new_prefix, max_depth));
                }
            }
            paths
        }
        ArcValue::Array(_) => {
            vec![prefix.to_vec()]
        }
        _ => {
            vec![prefix.to_vec()]
        }
    }
}
