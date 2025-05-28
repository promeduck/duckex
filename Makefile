.PHONY: bench help dev all clean

help:
	@make -qpRr | egrep -e '^[a-z].*:$$' | sed -e 's~:~~g' | sort

dev:
	ERL_AFLAGS="-kernel shell_history enabled" iex --name node@127.0.0.1 --cookie cookie -S mix

bench:
	mix run bench/echo.exs

RUST_TARGET_DIR = native/target/release
PRIV_DIR = priv
EXECUTABLE = duckex

all: $(PRIV_DIR)/$(EXECUTABLE)

$(PRIV_DIR)/$(EXECUTABLE): native/src/main.rs native/Cargo.toml
	@mkdir -p $(PRIV_DIR)
	cd native && cargo build --release
	cp $(RUST_TARGET_DIR)/$(EXECUTABLE) $(PRIV_DIR)/

clean:
	cd native && cargo clean
	rm -f $(PRIV_DIR)/$(EXECUTABLE)