{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    
    {# 1. Mengambil nama sub-folder langsung di bawah folder 'models' #}
    {% set folder_name = node.fqn[1] if node.fqn|length > 2 else none %}
    
    {# 2. Menentukan nama dasar (menggunakan alias kustom jika ada, jika tidak gunakan nama file) #}
    {% set base_name = custom_alias_name if custom_alias_name is not none else node.name %}

    {# 3. Jika file berada di sub-folder (seperti bronze, silver, gold), tambahkan prefix 'ch_ nama_folder_' #}
    {% if folder_name is not none and folder_name in ['bronze', 'silver', 'gold'] %}
        {{ return(folder_name ~ '_' ~ base_name | trim) }}
    {% else %}
        {# Jika file berada di root folder 'models' langsung, gunakan nama standar #}
        {{ return(base_name | trim) }}
    {% endif %}

{%- endmacro %}
