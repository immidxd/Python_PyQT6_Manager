#!/bin/bash

# Активуємо віртуальне середовище
source venv/bin/activate

# Встановлюємо змінні середовища для Qt
export QT_DEBUG_PLUGINS=1
export QT_QPA_PLATFORM=cocoa

# Використовуємо тільки плагіни з PyQt6, а не з Homebrew
SITE_PACKAGES="$(python -c 'import site; print(site.getsitepackages()[0])')"
export QT_PLUGIN_PATH="${SITE_PACKAGES}/PyQt6/Qt6/plugins"
export QT_QPA_PLATFORM_PLUGIN_PATH="${SITE_PACKAGES}/PyQt6/Qt6/plugins/platforms"

# Запускаємо програму за допомогою Python з віртуального середовища
python main.py 