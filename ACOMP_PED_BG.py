                color="Classificação",

                text_auto=True,
            )

            fig_classificacao.update_layout(
                showlegend=False
            )

            st.plotly_chart(
                fig_classificacao,
                use_container_width=True
            )


        with cl2:

            st.dataframe(

                classificacao_pedidos.sort_values(
                    "Pedido"
                ),

                use_container_width=True,

                hide_index=True,

                height=380,
            )


    # =====================================================
    # TABELA DE PEDIDOS
    # =====================================================

    st.divider()

    st.subheader(
        "📋 Pedidos"
    )

    st.dataframe(
        df_filtrado,
        use_container_width=True,
        hide_index=True
    )


    # =====================================================
    # DOWNLOAD CSV
    # =====================================================

    st.download_button(

        "⬇️ Baixar CSV",

        data=df_filtrado
        .to_csv(index=False)
        .encode("utf-8"),

        file_name="pedidos_wms.csv",

        mime="text/csv",
    )


    # =====================================================
    # TAREFAS GERADAS
    # =====================================================

    if not df_tarefas_filtrado.empty:

        st.divider()

        st.subheader(
            f"🗂️ Tarefas Geradas "
            f"(pedidos com status ≥ "
            f"{STATUS_MIN_TAREFAS})"
        )

        if tarefas_falhas:

            st.warning(

                f"⚠️ Não foi possível consultar "
                f"tarefas de "
                f"{len(tarefas_falhas)} pedido(s): "
                f"{tarefas_falhas}"
            )

        resumo_pedido = (

            df_tarefas_filtrado

            .groupby("Pedido")

            .size()

            .reset_index(
                name="Qtd. de Tarefas"
            )
        )

        st.dataframe(

            resumo_pedido.sort_values(
                "Pedido"
            ),

            use_container_width=True,

            hide_index=True
        )

        with st.expander(
            "Ver detalhamento das tarefas"
        ):

            st.dataframe(

                df_tarefas_filtrado,

                use_container_width=True,

                hide_index=True
            )

else:
