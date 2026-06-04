<?php

declare(strict_types=1);

/**
 * Runnable example for the skaidb PHP driver.
 *
 *     php example.php [host] [port] [user] [password]
 *     php example.php 192.168.7.117 7000 skaidb secret
 *
 * Omit user/password for a server with auth disabled (anonymous).
 */

require __DIR__ . '/src/Skaidb.php';

use Skaidb\Connection;
use Skaidb\SkaidbException;

$host = $argv[1] ?? 'localhost';
$port = isset($argv[2]) ? (int) $argv[2] : 7000;
$user = $argv[3] ?? 'anonymous';
$password = $argv[4] ?? '';

try {
    $db = new Connection($host, $port, $user, $password, 'QUORUM');
    echo "connected to {$host}:{$port}\n";

    // Best-effort cleanup of a leftover table from a previous run.
    try {
        $db->exec('DROP TABLE people');
    } catch (SkaidbException $e) {
        // table didn't exist — fine
    }

    // DDL — exec() returns affected rows (0 for DDL).
    $db->exec('CREATE TABLE people (PRIMARY KEY (id))');
    echo "created table people\n";

    // Prepared inserts with positional '?' placeholders.
    $insert = $db->prepare('INSERT INTO people (id, name, age) VALUES (?, ?, ?)');
    $seed = [
        [1, "Ada Lovelace", 36],
        [2, "Alan Turing", 41],
        [3, "Grace O'Brien", 28], // apostrophe is escaped safely
    ];
    foreach ($seed as $row) {
        $insert->execute($row);
    }
    echo "inserted " . count($seed) . " rows\n";

    // SELECT with a bound parameter.
    $stmt = $db->prepare('SELECT id, name, age FROM people WHERE age >= ?');
    $stmt->execute([30]);

    echo "columns: " . implode(', ', $stmt->columns()) . "\n";
    echo "rows (age >= 30):\n";
    foreach ($stmt->fetchAll() as $person) {
        printf("  #%d  %-16s age %d\n", $person['id'], $person['name'], $person['age']);
    }

    // fetchColumn convenience.
    $count = $db->query('SELECT COUNT(*) FROM people');
    echo "total people: " . $count->fetchColumn() . "\n";

    // Cleanup.
    $db->exec('DROP TABLE people');
    echo "dropped table people\n";

    $db->close();
    echo "done\n";
} catch (SkaidbException $e) {
    fwrite(STDERR, "skaidb error: " . $e->getMessage() . "\n");
    exit(1);
}
